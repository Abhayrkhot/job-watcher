#!/usr/bin/env python3
"""Poll public job feeds and email newly discovered matching roles."""

from __future__ import annotations

import gzip
import hashlib
import html
import http.client
import json
import os
import re
import smtplib
import socket
import sqlite3
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
DB_PATH = STATE_DIR / "jobs.sqlite3"
BOARDS_PATH = STATE_DIR / "ats_boards.json"
FEEDS_PATH = STATE_DIR / "feeds.json"
BOOTSTRAP_BOARDS_PATH = BASE_DIR / "bootstrap" / "ats_boards.json"
BOOTSTRAP_FEEDS_PATH = BASE_DIR / "bootstrap" / "feeds.json"
ATS_BATCH_SIZE = 2000
HOT_POLL_SECONDS = 2 * 60
BASE_POLL_SECONDS = 10 * 60
# Workday hosts throttle (1500+ tenants share a few hosts), so quiet tenants wait longer.
PROVIDER_BASE_POLL_SECONDS = {"workday": 20 * 60}
# launchd starts a missed run the moment the Mac wakes, often before DNS is back.
NETWORK_WAIT_SECONDS = 60
NETWORK_RETRY_SECONDS = 5
NETWORK_CHECK_HOSTS = ("boards-api.greenhouse.io", "api.lever.co", "api.ashbyhq.com")
# Companies often keep their slug when they switch ATS, so a board that starts
# returning 404 is probed under the same slug on the other public APIs.
MIGRATION_PROBE_URLS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{slug}",
    "lever": "https://api.lever.co/v0/postings/{slug}?mode=json&limit=1",
}
AGGREGATOR_INTERVAL_SECONDS = 15 * 60
AGGREGATOR_INTERVALS = {"jobicy": 6 * 60 * 60}
FEED_INTERVAL_SECONDS = 15 * 60
HEALTH_REPORT_INTERVAL_SECONDS = 24 * 60 * 60
HEARTBEAT_EXPECTED_SECONDS = 5 * 60
HEARTBEAT_GRACE_SECONDS = 10 * 60
MAX_JOB_AGE_SECONDS = 24 * 60 * 60
FUTURE_TIMESTAMP_GRACE_SECONDS = 5 * 60
# Postings that say they will not sponsor a work visa are dropped. Set
# JOB_WATCHER_KEEP_NO_SPONSORSHIP=1 to keep them.
FILTER_NO_SPONSORSHIP = os.environ.get("JOB_WATCHER_KEEP_NO_SPONSORSHIP") != "1"
SUPPORTED_PROVIDERS = {
    "ashby", "greenhouse", "lever", "lever-eu", "recruitee", "smartrecruiters",
    "workable", "workday", "oracle", "bamboohr", "rippling", "jobvite", "jazzhr",
}
# --- Role filter --------------------------------------------------------------
# Recall first: accept anything in the software / data / ML area and reject only
# clear disqualifiers (seniority, internships, 3+ years required, non-US,
# citizenship or clearance, excluded companies). Measured against a curated
# new-grad list with tools/eval_filter.py.

ROLE_NOUN_RE = re.compile(
    r"\b(?:engineers?|developers?|programmer|sde|swe|sdet|mts|technical staff|"
    r"(?:software|data|ml|machine learning|platform|security) engineering)\b"
)
# Analyst / associate / scientist titles span every field, so they need a
# software, data or ML word next to them.
WEAK_ROLE_NOUN_RE = re.compile(r"\b(?:scientists?|researchers?|analysts?|associates?)\b")
WEAK_ROLE_DOMAIN_RE = re.compile(
    r"\b(?:software|data|analytics|bi|business intelligence|machine learning|ml|ai|"
    r"artificial intelligence|deep learning|nlp|computer vision|quantitative|quant|"
    r"computational|applied|decision|full[- ]?stack|technology|product|"
    r"insights|statistic\w*)\b"
)
SELF_SUFFICIENT_ROLE_RE = re.compile(
    r"\b(?:developers?|programmer|sde|swe|sdet|mts|member of (?:the )?technical staff)\b"
)
DOMAIN_RE = re.compile(
    r"\b(?:software|sw|full[- ]?stack|back[- ]?end|front[- ]?end|web|mobile|ios|android|"
    r"apps?|application|applications|platforms?|infrastructure|infra|cloud|devops|"
    r"dev ops|sre|site reliability|production|systems software|embedded|firmware|"
    r"kernel|operating systems?|compilers?|gpu|cuda|hpc|performance|inference|"
    r"distributed|data|databases?|storage|analytics|bi|business intelligence|"
    r"machine learning|ml|mlops|ai|artificial intelligence|deep learning|llms?|"
    r"genai|generative|nlp|natural language|computer vision|vision|perception|"
    r"autonomy|autonomous|robotics|simulation|graphics|games?|gameplay|security|"
    r"cyber|cybersecurity|appsec|privacy|automation|qa|quality assurance|"
    r"solutions|forward[- ]deployed|deployed|deployment|integrations?|"
    r"implementation|tools|tooling|developer productivity|developer experience|"
    r"products?|growth|payments|blockchain|crypto|smart contracts?|quantitative|"
    r"quant|algorithms?|research|applied|decision|computational|"
    r"scientific computing|python|java|c\+\+|golang|rust|javascript|typescript|"
    r"react|node|salesforce|servicenow|search|ranking|recommendations?|"
    r"networking|network software|observability|reliability|generalist|"
    r"technology)\b"
)
HARDWARE_RE = re.compile(
    r"\b(?:lithography|semiconductor|wafer|metrology|defectivity|controls?|change control|"
    r"product development|r&d|wireless|lab|laboratory|mechanical|civil|structural|electrical|electronics?|hardware|"
    r"manufacturing|process|chemical|rf|analog|mixed[- ]signal|asic|fpga|soc|"
    r"silicon|vlsi|layout|pcb|packaging|materials|industrial|environmental|"
    r"facilities|facility|physical|construction|field|propulsion|avionics|thermal|optical|optics|"
    r"photonics|mechatronics|power|battery|hvac|geotechnical|transportation|"
    r"traffic|water|mining|petroleum|nuclear|biomedical|clinical|quality|test|"
    r"validation|verification|sales|service|maintenance|reliability)\b"
)
SOFTWARE_CORE_RE = re.compile(
    r"\b(?:software|sw|full[- ]?stack|back[- ]?end|front[- ]?end|web|mobile|cloud|"
    r"devops|sre|site reliability|data|machine learning|ml|ai|llms?|"
    r"firmware|embedded software|test automation|sdet|quality assurance|"
    r"developers?|programmer|python|java|c\+\+|bi)\b"
)
EXCLUDED_TITLE_RE = re.compile(
    r"\b(?:intern|interns|internship|internships|co-?op|senior|sr|staff|principal|"
    r"lead|leader|manager|director|head|vp|vice president|chief|cto|distinguished|"
    r"professor|lecturer|postdoc(?:toral)?|post-doc|technician|recruiter|"
    r"account executive|experienced|expert|mid[- ]?level|iii|iv|v|vi|3|4|5|contractor|"
    r"part[- ]time|fixed[- ]term|temporary|temp|annotat\w*|tutor|raters?|speakers?|"
    r"contract (?:student|worker|role|position)|extern|externship|flexible hours|"
    r"avp|svp|evp|assistant vice president|post doc|administrator|coordinator|assistant|"
    r"freelance|hourly|summer|mba|seasonal)\b|\(contract\)|- contract\b"
)
ANALYST_DIRECT_RE = re.compile(
    r"\b(?:data|analytics|bi|business intelligence|quantitative|quant|statistical|insights|"
    r"data science|machine learning|ml)\s+(?:\w+\s+)?analysts?\b"
)
ANALYST_TRAILING_RE = re.compile(
    r"\banalysts?\s*[,\-–:(/]\s*(?:[\w&\-]+\s+){0,4}?"
    r"(?:data|analytics|full[- ]?stack|software|machine learning)\b"
)
ASSOCIATE_DIRECT_RE = re.compile(r"\b(?:software|data|engineering|analytics|technology)\s+associates?\b")
BUSINESS_RE = re.compile(
    r"\b(?:facility|facilities|finance|financial|vendor|procurement|strategy|strategic|"
    r"business|marketing|operations|sales|compliance|risk|audit|accounting|tax|hr|"
    r"human resources|real estate|supply chain|legal|privacy)\b"
)
SCIENCE_DOMAIN_RE = re.compile(
    r"\b(?:data|machine learning|ml|ai|artificial intelligence|deep learning|nlp|"
    r"computer vision|vision|robotics|computational|applied|decision|research|quantitative)\b"
)
NON_TECH_SCIENCE_RE = re.compile(
    r"\b(?:bio\w*|chem\w*|pharma\w*|drug|clinical|lab|laboratory|lcms|assay|humanities|"
    r"arts|ethics|social|psych\w*|materials?|protein|genomic\w*|cell|immun\w*|pathology|"
    r"toxicology|formulation|analytical|bioanalytical|physics|environmental|agricultur\w*|food)\b"
)
ML_STRONG_RE = re.compile(
    r"\b(?:machine learning|ml|ai|artificial intelligence|deep learning|computational|nlp|"
    r"computer vision)\b"
)
TECHNICAL_STAFF_RE = re.compile(r"\b(?:member of (?:the )?)?technical staff\b")
NEW_GRAD_TITLE_RE = re.compile(
    r"new grad|new college grad|university grad|graduate|early career|"
    r"entry[- ]level|campus|class of 20\d\d|junior|\bassociate\b|early talent"
)
# Your standing exclusions: prop/quant trading, ByteDance/TikTok, aerospace and
# defense. Extend or trim in state/excluded_companies.json (a JSON list of names).
DEFAULT_EXCLUDED_COMPANIES = (
    # proprietary and quant trading
    "Hudson River Trading", "HRT", "Five Rings", "Susquehanna",
    "Susquehanna International Group", "SIG", "Optiver", "Belvedere Trading",
    "Jane Street", "Citadel", "Citadel Securities", "DRW", "Jump Trading",
    "Wolverine Trading", "IMC", "IMC Trading", "Headlands Technologies", "Akuna Capital",
    "Old Mission", "Virtu", "Virtu Financial", "Flow Traders", "Tower Research Capital",
    "XTX Markets", "Radix Trading", "Chicago Trading Company", "PEAK6", "Maven Securities",
    "Da Vinci", "Tradebot", "Two Sigma", "D. E. Shaw", "DE Shaw", "Renaissance Technologies",
    "Hudsonrivertrading", "Fiverings", "Janestreet", "Jumptrading", "Belvederetrading",
    # ByteDance / TikTok
    "ByteDance", "TikTok",
    # aerospace and defense
    "SpaceX", "Anduril", "Anduril Industries", "Andurilindustries", "Shield AI", "Shieldai",
    "Saronic", "Lockheed Martin", "Northrop Grumman", "Raytheon", "RTX", "Boeing",
    "General Dynamics", "L3Harris", "BAE Systems", "Leidos", "Peraton",
    "Booz Allen Hamilton", "CACI", "SAIC", "ManTech", "Huntington Ingalls", "Textron",
    "Rocket Lab", "Rocketlab", "Relativity Space", "Relativity", "Blue Origin",
    "United Launch Alliance", "ULA", "Sierra Space", "Firefly Aerospace", "Astranis",
    "Palantir", "Palantir Technologies", "Accenture Federal Services",
    "Accenturefederalservices", "CGS Federal", "Cgsfederal", "Voyager Technologies",
    "Voyagertechnologiesinc", "Epirus", "Castelion", "Hadrian", "Skydio", "Hermeus",
    "Joby Aviation", "Archer Aviation", "Boom Supersonic", "Varda Space", "K2 Space",
    "Apex Space", "Vast", "Shield", "f7d8749a-72d6-407c-bf98-292ea630c73a",
    "Fantom Corporation", "Graham Capital Management", "Grahamcapitalmanagement", "Amentum",
    # intelligence and defense contractors whose roles need a clearance
    "Wyetech", "Agile Defense", "Agiledefense", "BTI360", "Bti36021", "GRVTY",
    "Freedom Consulting Group", "Freedomconsulting", "Anavation", "Anavationllc",
    "Honeywell Aerospace",
)
EXCLUDED_COMPANIES_PATH = STATE_DIR / "excluded_companies.json"
US_STATE_CODES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|"
    "MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|"
    "VT|VA|WA|WV|WI|WY|DC"
)
US_STATE_CODE_SET = set(US_STATE_CODES.split("|"))
# Case-sensitive on purpose: "Remote in Canada" must not read as Indiana.
US_STATE_CODE_RE = re.compile(rf",\s*(?:{US_STATE_CODES})\b")
US_LOCATION_RE = re.compile(
    r"\b(?:united states|usa|u\.s\.a?\.?|us|america|"
    r"alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|"
    r"florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|"
    r"maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|"
    r"nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|"
    r"north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|"
    r"south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|"
    r"wisconsin|wyoming|"
    r"nyc|sf|bay area|silicon valley|san francisco|seattle|boston|austin|chicago|"
    r"los angeles|san diego|san jose|palo alto|mountain view|menlo park|sunnyvale|"
    r"santa clara|redwood city|oakland|berkeley|redmond|bellevue|kirkland|denver|"
    r"boulder|atlanta|miami|dallas|houston|pittsburgh|philadelphia|baltimore|"
    r"raleigh|durham|nashville|salt lake city|phoenix|portland|minneapolis|detroit|"
    r"ann arbor|madison|columbus|cambridge, ma|somerville|arlington, va|"
    r"jersey city|hoboken|brooklyn|manhattan|irvine|santa monica|culver city)\b",
    re.I,
)
NON_US_LOCATION_RE = re.compile(
    r"\b(?:canada|toronto|vancouver|montreal|ontario|quebec|united kingdom|uk|"
    r"england|london|ireland|dublin|germany|berlin|munich|france|paris|spain|"
    r"madrid|barcelona|portugal|lisbon|netherlands|amsterdam|poland|warsaw|"
    r"switzerland|zurich|sweden|stockholm|denmark|norway|finland|israel|tel aviv|"
    r"india|bangalore|bengaluru|hyderabad|pune|chennai|delhi|mumbai|singapore|"
    r"japan|tokyo|korea|seoul|china|beijing|shanghai|shenzhen|hong kong|taiwan|"
    r"taipei|australia|sydney|melbourne|new zealand|brazil|sao paulo|mexico|"
    r"argentina|colombia|chile|emea|apac|latam|europe|eu|philippines|vietnam|"
    r"indonesia|malaysia|thailand|pakistan|nigeria|kenya|egypt|south africa|uae|"
    r"dubai|saudi|qatar|turkey|istanbul|romania|bucharest|ukraine|czech|prague|"
    r"hungary|budapest|greece|italy|milan|rome|belgium|brussels|austria|vienna|"
    r"estonia|serbia|belgrade|costa rica|peru|"
    r"noida|gurgaon|gurugram|kolkata|ahmedabad|kochi|cochin|coimbatore|trivandrum|"
    r"thiruvananthapuram|mysore|mysuru|indore|jaipur|chandigarh|vadodara|nagpur|"
    r"visakhapatnam|bhubaneswar|karnataka|maharashtra|telangana|tamil nadu|uttar pradesh|"
    r"haryana|kerala|gujarat|west bengal|andhra pradesh|bangladesh|dhaka|sri lanka|colombo|"
    r"nepal|kathmandu|karachi|lahore|islamabad|manila|cebu|lagos|nairobi|cairo|krakow|"
    r"wroclaw|cluj|sofia|bulgaria|lithuania|vilnius|latvia|riga|croatia|zagreb|slovakia|"
    r"bratislava|slovenia|guadalajara|monterrey|bogota|medellin|buenos aires|santiago|"
    r"montevideo|uruguay|kuala lumpur|jakarta|ho chi minh|hanoi|bangkok|"
    r"ind|gbr|deu|fra|esp|pol|mex|bra|chn|jpn|kor|sgp|aus|irl|nld|isr|phl|vnm|rou|hun|"
    r"cze|che|swe|nor|dnk|fin|bel|aut|ita|prt|tur|egy|zaf|nga|ken|pak|bgd|lka|mys|tha|"
    r"idn|arg|chl|cri)\b",
    re.I,
)
GENERIC_LOCATION_RE = re.compile(
    r"^(?:remote|anywhere|hybrid|flexible|distributed|virtual|work from home|wfh|"
    r"multiple locations|various locations|\d+ locations|remote[- ]first|tbd|)$",
    re.I,
)
ENTRY_DESCRIPTION_RE = re.compile(
    r"\b(?:new|recent) (?:college |university )?grad(?:uate)?s?\b|\bearly career\b|"
    r"\bentry[ -]level\b|\b0\s*(?:-|to)\s*[123] years?\b|"
    r"\b1\s*(?:-|to)\s*[23] years?\b|\b(?:up to|less than) [23] years?\b|"
    r"\bno (?:prior |professional )?experience (?:is )?(?:required|necessary)\b|"
    r"\bclass of 20\d\d\b|\bgraduating (?:in|by) 20\d\d\b",
    re.I,
)
_US = r"(?:u\.?\s?s\.?|united states)"
CITIZENSHIP_REQUIREMENT_RE = re.compile(
    rf"\b(?:must|required to|need to|needs to)\s+(?:be|hold|have|possess)\s+"
    rf"(?:a\s+|an\s+)?(?:{_US}\s+)?citizen(?:s|ship)?\b"
    rf"|\b{_US}\s+citizenship\s+(?:is\s+)?(?:required|a requirement|mandatory)\b"
    rf"|\brequires?\s+{_US}\s+citizenship\b"
    rf"|\b{_US}\s+citizens?\s+only\b|\bonly\s+(?:{_US}\s+)?citizens\b"
    rf"|\bcitizenship\s+(?:is\s+)?required\b"
    rf"|\b{_US}\s+citizen(?:ship|s)?\s+or\s+(?:lawful\s+)?permanent\s+residen"
    rf"|\b(?:active|current)\s+(?:(?:top\s+)?secret|ts/sci|security|government)\s+clearance\b"
    rf"|\b(?:ability|able)\s+to\s+(?:obtain|acquire|get)\s+(?:and\s+maintain\s+)?"
    rf"(?:a\s+|an\s+)?(?:{_US}\s+)?(?:government\s+)?(?:security\s+)?clearance\b"
    rf"|\bmust\s+be\s+(?:a\s+)?{_US}\s+persons?\b|\b{_US}\s+person\s+(?:status\s+)?(?:is\s+)?required\b"
    # Clearances and the programs that imply them ("TS/SCI with Full Scope Polygraph",
    # "Active DoD Secret", "TS Clearance", "Public Trust clearance").
    rf"|\bts\s*/\s*sci\b|\btop\s+secret\b|\bpolygraph\b|\b(?:full[- ]scope|ci)\s+poly\b|\+\s*poly\b"
    rf"|\bts\s+clearance\b"
    rf"|\b(?:active|current)\s+(?:(?:dod|doe|dhs|u\.?s\.?|government|federal)\s+){{1,2}}secret\b"
    rf"|\b(?:active|current)\s+secret\s+(?:clearance|level|or\s+higher)\b"
    rf"|\bpublic\s+trust\s+(?:clearance|position|investigation|suitability|background|level|eligib)"
    rf"|\b(?:obtain|maintain|hold|possess)\s+(?:and\s+maintain\s+)?(?:a\s+|an\s+)?(?:active\s+)?public\s+trust\b"
    rf"|\b(?:ability|able|eligible)\s+to\s+(?:obtain|acquire|get)\s+(?:and\s+maintain\s+)?"
    rf"(?:a\s+|an\s+|the\s+)?(?:required\s+)?(?:customer\s+|government\s+|federal\s+|dod\s+)?"
    rf"(?:security\s+)?clearance\b"
    # "United States Citizenship and position-appropriate security clearance are required"
    rf"|\b{_US}\s+citizenship\b[^.;?]{{0,80}}?\b(?:is|are)\s+required\b"
    # ITAR and export control: "only open to U.S. Citizens", "only to U.S. Persons"
    rf"|\b(?:only|exclusively)\s+(?:be\s+)?(?:open|available)\s+to\s+(?:{_US}\s+)?citizens\b"
    rf"|\b(?:only|must|required|limited)\b[^.;?]{{0,80}}?\b{_US}\s+persons?\b"
    rf"|\b{_US}\s+persons?\b[^.;?]{{0,40}}?\b(?:only|required)\b"
    rf"|\btemporary\s+visas?\b[^.;?]{{0,120}}?\b(?:not|in)eligible",
    re.I,
)
# Work authorization without sponsorship. A posting that says so rules out anyone
# on a student visa, OPT or an H-1B, so it is rejected like a citizenship rule.
_SPONSOR = r"(?:visa\s+|immigration\s+|employment\s+|work\s+|h-?1b\s+)?sponsor(?:ship|s|ing)?\b"
_SPONSOR_NEGATION = (
    r"(?:does not|do not|did not|will not|would not|cannot|can not|can't|won't|doesn't|"
    r"don't|is not|are not|isn't|unable to|not able to|no longer|not currently|"
    r"not in a position to)"
)
NO_SPONSORSHIP_RE = re.compile(
    # "does not offer support or sponsorship", "unable to provide visa sponsorship"
    rf"\b{_SPONSOR_NEGATION}\s+(?:(?!require|need)[\w/-]+\s+){{0,4}}?{_SPONSOR}"
    # "Visa sponsorship is not available", "sponsorship will not be provided"
    rf"|\b{_SPONSOR}\s*(?:is|are|will)?\s*(?:not|un)\s*(?:be\s+)?"
    rf"(?:available|offered|provided|possible|supported|considered)"
    # "No sponsorship offered" (but not "no sponsorship information" or "no sponsorship needed")
    rf"|\bno\s+{_SPONSOR}(?!\s+(?:is\s+)?(?:required|needed|necessary|information|info|details|"
    rf"mention|language|data)\b)"
    # "without the need for employer sponsorship", "without current or future sponsorship"
    rf"|\bwithout\s+(?:the\s+|a\s+)?(?:(?:need|requirement)\s+(?:for|of)\s+)?(?:[\w/-]+\s+){{0,4}}?{_SPONSOR}"
    rf"|\bnot\s+(?:be\s+)?eligible\s+for\s+(?:[\w/-]+\s+){{0,2}}?{_SPONSOR}"
    # "must not require sponsorship", "and not require Toyota support or sponsorship"
    rf"|\b(?:must|should|and|who do|will)\s+not\s+(?:now\s+or\s+in\s+the\s+future\s+)?"
    rf"(?:require|need)\b[^.;?]{{0,50}}?\bsponsor"
    # "unable to offer employment to ... those who require sponsorship"
    rf"|\b{_SPONSOR_NEGATION}\s+(?:\w+\s+){{0,3}}?(?:hire|employ|consider|offer employment)\b"
    rf"[^.;?]{{0,120}}?\b(?:require|need)s?\b[^.;?]{{0,40}}?\bsponsor"
    rf"|\b{_SPONSOR}\s*:\s*(?:no|none|not available|unavailable)\b"
    rf"|\bunable to (?:sponsor|transfer)\b",
    re.I,
)
SPONSORSHIP_OFFERED_RE = re.compile(
    rf"\b(?:we|company|employer)\s+(?:do|does|will|can|may|are able to|is able to)\s+(?:also\s+)?sponsor"
    rf"|\b{_SPONSOR}\s+(?:is\s+|may\s+be\s+)?(?:available|offered|provided|supported)\b"
    rf"|\b(?:offer|offers|provide|provides)\s+(?:\w+\s+){{0,2}}?{_SPONSOR}"
    rf"|\bsupport\s+h-?1b\b|\bh-?1b\s+transfers?\s+(?:are\s+)?(?:welcome|supported)",
    re.I,
)
NEGATED_BEFORE_RE = re.compile(r"(?:\b(?:not|no|unable|cannot|never|without)\b|n't)[^.;?]{0,30}$", re.I)
FORM_QUESTION_RE = re.compile(r"[^.!;\n]*\?")
_WORD_NUMBER_NAMES = (
    "zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen"
).split()
WORD_NUMBERS = {name: value for value, name in enumerate(_WORD_NUMBER_NAMES)}
_NUMBER = rf"\d{{1,2}}|{'|'.join(_WORD_NUMBER_NAMES)}"
YEARS_RE = re.compile(
    rf"\b(?P<low>{_NUMBER})\s*"
    rf"(?:\+|plus)?\s*(?:(?:-|–|to)\s*(?P<high>{_NUMBER})\s*\+?\s*)?(?:years?|yrs?)\b",
    re.I,
)
# "three (3) years" and "3 (three) years" read as "3 years".
PAREN_NUMBER_RE = re.compile(
    rf"\b(?:(?:{'|'.join(_WORD_NUMBER_NAMES)})\s*\((\d{{1,2}})\+?\)|(\d{{1,2}})\s*\((?:{'|'.join(_WORD_NUMBER_NAMES)})\))",
    re.I,
)
# Cues read around each figure, not across the whole sentence, so "ideally at a
# startup" after "4+ years ..." does not make the years optional.
SOFT_BEFORE_RE = re.compile(
    r"(?:\b(?:preferred|nice[- ]to[- ]have|bonus(?: points)?|desired|desirable|a plus)\b[^.;]{0,30}"
    r"|\b(?:ideally|preferably|optionally)\b[^.;,]{0,15})$",
    re.I,
)
SOFT_AFTER_RE = re.compile(
    r"\b(?:preferred|preferable|(?:is|are|a|big)\s+plus|nice[- ]to[- ]have|desired|desirable|bonus)\b",
    re.I,
)
HARD_BEFORE_RE = re.compile(
    r"\b(?:minimum(?:\s+of)?|at\s+least|required|requires?|must\s+have|needs?)\s*:?\s*"
    r"(?:a\s+)?(?:minimum\s+of\s+)?$",
    re.I,
)
HARD_AFTER_RE = re.compile(r"^\s*(?:of\s+[^.;]{0,60}?)?\b(?:is\s+|are\s+)?(?:required|mandatory)\b", re.I)
# Figures that are not an experience requirement: "less than 3 years", "within the
# last five years", "after 3 years", "a 4 year university", "18 years of age".
NOT_A_REQUIREMENT_BEFORE_RE = re.compile(
    r"\b(?:less than|fewer than|under|up to|no more than|at most|maximum of|after|every|each|"
    r"(?:within|in|during|for)\s+(?:the\s+|your\s+)?(?:past|last|next|first|initial))\s*$",
    re.I,
)
NOT_A_REQUIREMENT_AFTER_RE = re.compile(
    r"^[\s-]*(?:old\b|of age\b|in a row\b|in business\b|(?:accredited\s+)?"
    r"(?:college|university|degree|bachelor|program|institution|school)\b)",
    re.I,
)
REQUIREMENT_CUE_RE = re.compile(
    r"experien|required|requires|requirement|minimum|at least|must have",
    re.I,
)
COMPANY_HISTORY_RE = re.compile(
    r"(?:over|more than|nearly|almost|for (?:the )?(?:past|last)|around for|in business for|"
    r"founded|since|history of)\s*$", re.I
)
_N = rf"({_NUMBER})"
# Tiered requirements ("BS + 6 years OR MS + 5 years OR PhD"): the Master's path applies.
MASTERS_YEARS_RE = re.compile(
    rf"\b(?:master'?s|ms|m\.s\.|msc)\b[^.;]{{0,60}}?\b{_N}\s*\+?\s*(?:years?|yrs)\b"
    rf"|\b{_N}\s*\+?\s*(?:years?|yrs)\b[^.;]{{0,40}}?\b(?:with|and|plus)\s+(?:an?\s+)?"
    rf"(?:master'?s|ms|m\.s\.)\b",
    re.I,
)
DEGREE_OR_YEARS_RE = re.compile(
    rf"\bdegree\b[^.;]{{0,80}}?\bor\s+(?:a\s+)?(?:minimum\s+of\s+|at\s+least\s+)?{_N}\s*\+?\s*(?:years?|yrs)"
    r"|\b(?:may|can)\s+be\s+substituted\b|\bin\s+lieu\s+of\s+(?:a\s+|the\s+)?(?:\w+\s+){0,2}?degree\b",
    re.I,
)
MASTERS_ALTERNATIVE_RE = re.compile(
    r"\b(?:or|with)\s+(?:an?\s+|the\s+)?(?:master'?s|ms|m\.s\.|mba|ph\.?d\.?|doctorate|"
    r"graduate degree|advanced degree)\b",
    re.I,
)
NEW_GRAD_CREDIT_RE = re.compile(
    r"\binternships?\b|\bco-?ops?\b|\bacademic\b|\bcoursework\b|\bresearch experience\b", re.I
)
SEPARATE_TRACK_RE = re.compile(r"\bexperienced hires?\b|\bfor senior\b", re.I)
# Leveled postings ("Level 1 - no experience required. Level 2 - Two years ...
# Level 3 - Four years ..."): the higher levels are a separate track.
HIGHER_LEVEL_RE = re.compile(
    r"^\W*(?:for\s+)?(?:level|tier|grade)\s+(?:[2-9]|ii|iii|iv|v|two|three|four|five)\b", re.I
)
PREFERRED_CUE_RE = re.compile(
    r"\b(?:preferred|nice[- ]to[- ]have|bonus|(?:is|are|a|big) plus|ideally|desired|"
    r"desirable|preferably)\b",
    re.I,
)
# Sentences where the years are an alternative a new grad can satisfy:
# "or a Master's", "with a PhD", "degree or 4 years", "internships count".
NEW_GRAD_SUBSTITUTE_RE = re.compile(
    r"\b(?:or|with)\s+(?:an?\s+|the\s+)?(?:master'?s|ms|m\.s\.|mba|ph\.?d\.?|doctorate|"
    r"graduate degree|advanced degree)\b"
    r"|\bdegree\b[^.;]{0,80}?\bor\s+(?:\d{1,2}|one|two|three|four|five|six)\s*\+?\s*(?:years?|yrs)"
    r"|\binternships?\b|\bco-?ops?\b|\bacademic\b|\bcoursework\b|\bresearch experience\b"
    r"|\bexperienced hires?\b|\bfor senior\b",
    re.I,
)
SENIOR_PHRASE_RE = re.compile(
    r"\bsenior[- ]level experience\b|\bseasoned (?:software )?engineer\b|"
    r"\bmid[- ]?(?:senior[- ])?level\b|\bmid[- ]senior\b|\b(?:several|many|multiple) years\b|"
    r"\bproven track record\b|"
    r"\b(?:deep|extensive|significant|substantial) (?:professional |industry |hands-on )?"
    r"(?:expertise|experience) (?:in|with|building|designing|developing|leading|shipping|"
    r"working|writing|running|operating|delivering)\b|"
    r"\b(?:an?|seeking|looking for)\s+(?:highly\s+)?experienced\s+"
    r"(?:software\s+|backend\s+|full[- ]stack\s+)?engineer\b(?!s)",
    re.I,
)
OPTIONAL_HEADER_RE = re.compile(
    r"\b(?:(?:preferred|desired|bonus|additional|nice[- ]to[- ]have)\s+(?:qualifications|skills|"
    r"requirements|experience|points)|nice[- ]to[- ]haves?|bonus points|considerations for top "
    r"candidates|it(?:'s| is| would be) a plus if|pluses)\b\s*:?",
    re.I,
)
REQUIRED_HEADER_RE = re.compile(
    r"\b(?:(?:basic|minimum|required|must[- ]have)\s+(?:qualifications|skills|requirements|"
    r"experience)|qualifications|requirements|what you(?:'ll| will) need(?: to succeed)?|"
    r"what we(?:'re| are) looking for|who you are|about you)\b\s*:?",
    re.I,
)
# One pass, optional headers first, so "Preferred Qualifications" is never read as
# a bare "Qualifications" header.
SECTION_HEADER_RE = re.compile(
    f"(?P<optional>{OPTIONAL_HEADER_RE.pattern})|(?P<required>{REQUIRED_HEADER_RE.pattern})", re.I
)
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?;])\s+|\n+|\s+[•·▪]\s*|\s+-\s+(?=[A-Z])")


def normalize_title(title: object) -> str:
    text = html.unescape(str(title or "")).lower().replace(" ", " ")
    text = re.sub(r"[‐-―−]", "-", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_company(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def load_excluded_companies() -> set[str]:
    names = list(DEFAULT_EXCLUDED_COMPANIES)
    try:
        if EXCLUDED_COMPANIES_PATH.exists():
            names = json.loads(EXCLUDED_COMPANIES_PATH.read_text())
    except (OSError, ValueError) as exc:
        print(f"job-watcher: ignoring unreadable {EXCLUDED_COMPANIES_PATH}: {exc}", file=sys.stderr)
    return {normalize_company(name) for name in names if normalize_company(name)}


EXCLUDED_COMPANIES = load_excluded_companies()


def is_excluded_company(job: dict) -> bool:
    candidates = [job.get("company_name")]
    board_key = str(job.get("board_key") or "")
    if ":" in board_key:
        candidates.append(board_key.split(":", 1)[1])
    return any(normalize_company(value) in EXCLUDED_COMPANIES for value in candidates if value)


def in_role_family(title: object) -> bool:
    text = normalize_title(title)
    core_text = re.sub(r"\bdata ?cent(?:er|re)s?\b", " ", text)
    if HARDWARE_RE.search(text) and not SOFTWARE_CORE_RE.search(core_text):
        return False
    if ROLE_NOUN_RE.search(text):
        return bool(SELF_SUFFICIENT_ROLE_RE.search(text) or DOMAIN_RE.search(text))
    nouns = {match.group(0).rstrip("s") for match in WEAK_ROLE_NOUN_RE.finditer(text)}
    if not nouns:
        return False
    if "analyst" in nouns:
        # "Data Analyst", "Analytics Analyst", "Analyst, Data Systems" -- not
        # "Facility Analyst" or "Business Analyst, Finance AI".
        trailing = ANALYST_TRAILING_RE.search(text)
        if ANALYST_DIRECT_RE.search(text) or (trailing and not BUSINESS_RE.search(text)):
            return True
    if "associate" in nouns and ASSOCIATE_DIRECT_RE.search(text):
        return True
    if nouns & {"scientist", "researcher"} and SCIENCE_DOMAIN_RE.search(text):
        # Lab, pharma and humanities scientists need an ML or computational angle.
        return not (NON_TECH_SCIENCE_RE.search(text) and not ML_STRONG_RE.search(text))
    return False


def has_excluded_title(title: object) -> bool:
    text = TECHNICAL_STAFF_RE.sub(" ", normalize_title(title))
    return bool(EXCLUDED_TITLE_RE.search(text))


def title_rejection(title: object) -> str | None:
    if has_excluded_title(title):
        return "excluded_seniority_or_internship"
    if not in_role_family(title):
        return "outside_role_families"
    return None


# A "Remote" or blank location counts only if the posting itself says US.
US_REMOTE_EVIDENCE_RE = re.compile(
    r"\b(?:united states|u\.s\.|usa|us[- ]based|based in the us|(?:anywhere|located|reside|"
    r"residing) (?:in|within) the (?:us|u\.s\.|united states)|within the (?:us|u\.s\.)|"
    r"us time ?zones?|(?:eastern|pacific|central|mountain) time|est|pst|cst)\b",
    re.I,
)
EXPLICIT_US_RE = re.compile(r"\b(?:united states|usa|u\.s\.a?\.?)\b", re.I)


def location_is_us(value: str, allow_bare_code: bool = True) -> bool:
    if EXPLICIT_US_RE.search(value):
        return True
    if NON_US_LOCATION_RE.search(value):
        return False  # "Noida, Uttar Pradesh, India" is not Indiana
    if allow_bare_code and value.strip(" .") in US_STATE_CODE_SET:
        return True
    return bool(US_STATE_CODE_RE.search(value) or US_LOCATION_RE.search(value))


def is_us_location(locations: list[str], description: str | None = None) -> bool:
    values = [re.sub(r"\s+", " ", str(value)).strip() for value in locations or []]
    foreign = any(NON_US_LOCATION_RE.search(value) for value in values)
    if any(location_is_us(value, allow_bare_code=not foreign) for value in values):
        return True
    if foreign:
        return False
    # Only "Remote", "Hybrid" or nothing at all: keep it only if the posting says US.
    if not all(GENERIC_LOCATION_RE.match(value.strip(" ,;()-")) for value in values):
        return False
    return bool(description and US_REMOTE_EVIDENCE_RE.search(description))


def stated_matches(pattern: re.Pattern, text: str):
    """Matches that are statements, skipping application-form questions such as
    "Will you now or in the future require sponsorship?"."""
    for match in pattern.finditer(text):
        if not FORM_QUESTION_RE.match(text, match.end(), match.end() + 200):
            yield match


def description_mentions_citizenship_requirement(description: str) -> bool:
    """False when the posting requires citizenship, a US-person status or a clearance."""
    text = description or ""
    for match in stated_matches(CITIZENSHIP_REQUIREMENT_RE, text):
        # "which may include meeting the definition of a U.S. Person" is a maybe.
        if not re.search(r"\bmay\b", text[max(0, match.start() - 60): match.end()], re.I):
            return False
    return True


def refuses_sponsorship(description: str) -> bool:
    """True when the posting says it will not sponsor a work visa ("Visa sponsorship
    is not available", "must be authorized to work without sponsorship"), unless it
    also says it does sponsor ("We do sponsor visas!")."""
    text = description or ""
    if next(stated_matches(NO_SPONSORSHIP_RE, text), None) is None:
        return False
    offered = (
        match for match in SPONSORSHIP_OFFERED_RE.finditer(text)
        if not NEGATED_BEFORE_RE.search(text[max(0, match.start() - 40): match.start()])
    )
    return next(offered, None) is None


def potential_match(job: dict) -> bool:
    return rejection_reason(job) not in {
        "inactive", "missing_posted_time", "outside_24_hours", "excluded_company",
        "excluded_seniority_or_internship", "outside_role_families", "outside_us",
    }


def _number(raw: str) -> int:
    raw = raw.lower()
    return WORD_NUMBERS[raw] if raw in WORD_NUMBERS else int(raw)


def requirement_sentences(text: str):
    """Yield (sentence, optional) pairs. Text under a "Preferred qualifications",
    "Nice to have" or similar header is optional until the next required header,
    even when the posting runs its sections together without periods."""
    text = PAREN_NUMBER_RE.sub(lambda m: m.group(1) or m.group(2), text or "")
    marked = SECTION_HEADER_RE.sub(
        lambda m: ("\n\u0001" if m.group("optional") else "\n\u0002") + m.group(0), text
    )
    optional = False
    for sentence in SENTENCE_SPLIT_RE.split(marked):
        if sentence.startswith("\u0001"):
            optional = True
        elif sentence.startswith("\u0002"):
            optional = False
        sentence = sentence.lstrip("\u0001\u0002").strip()
        if sentence:
            yield sentence, optional


def sentence_required_years(sentence: str, strict_cues: bool = False) -> int | None:
    """Years a sentence demands of a Master's new grad, or None if it demands none."""
    if (SEPARATE_TRACK_RE.search(sentence) or HIGHER_LEVEL_RE.search(sentence)
            or sentence.rstrip().endswith("?")):
        return None  # another track, or an application question
    masters = MASTERS_YEARS_RE.search(sentence)
    if masters and not PREFERRED_CUE_RE.search(sentence):
        return _number(next(group for group in masters.groups() if group))
    if DEGREE_OR_YEARS_RE.search(sentence) or MASTERS_ALTERNATIVE_RE.search(sentence):
        return None  # the degree on its own qualifies
    years, cued = [], []
    figures = list(YEARS_RE.finditer(sentence))
    if re.match(r"^\W*or\b", sentence, re.I) or len(figures) >= 4:
        return None
    for index, match in enumerate(figures):
        low = _number(match.group("low"))
        high = match.group("high")
        start, end = match.start(), match.end()
        before = sentence[max(0, start - 45): start]
        following = figures[index + 1].start() if index + 1 < len(figures) else len(sentence)
        # "2 years required/ 4 years preferred": each figure keeps its own cue.
        after = re.split(r"[/|]", sentence[end: min(following, end + 100)], maxsplit=1)[0]
        if (low > 15 or COMPANY_HISTORY_RE.search(sentence[max(0, start - 30): start])
                or NOT_A_REQUIREMENT_BEFORE_RE.search(before) or NOT_A_REQUIREMENT_AFTER_RE.match(after)):
            continue
        hard = HARD_BEFORE_RE.search(sentence[max(0, start - 25): start]) or HARD_AFTER_RE.match(after)
        if not hard and (SOFT_BEFORE_RE.search(before) or SOFT_AFTER_RE.search(after)):
            continue  # "3+ years ... is a plus", "ideally 2 years"
        if high and low == 2 and _number(high) <= 3:
            low = 1  # "2-3 years" is a junior band
        phrase = match.group(0)
        window = sentence[max(0, match.start() - 50): match.end() + 80]
        if REQUIREMENT_CUE_RE.search(window):
            cued.append(low)
            years.append(low)
        elif re.search(r"\+|plus|[-\u2013]|\bto\b", phrase):
            years.append(low)  # "3+ years building ..." is a requirement too
    if not years:
        return None
    if NEW_GRAD_CREDIT_RE.search(sentence) and max(years) < 3:
        return None  # internships and research count
    if strict_cues:
        return max(cued) if cued else None
    return max(years)


def required_years(text: str, strict_cues: bool = False) -> int | None:
    """The strictest years-of-experience requirement in the posting. With
    strict_cues, only figures tied to "experience", "required", "minimum" count."""
    found = []
    under_higher_level = False
    for sentence, optional in requirement_sentences(text):
        if under_higher_level:  # the sentence after a bare "Level 3 -" belongs to it
            under_higher_level = False
            continue
        if HIGHER_LEVEL_RE.search(sentence):
            under_higher_level = len(sentence) < 20
            continue
        if not optional and (years := sentence_required_years(sentence, strict_cues)) is not None:
            found.append(years)
    return max(found) if found else None


def asks_for_too_much_experience(text: str, title: object = "") -> bool:
    new_grad_title = bool(NEW_GRAD_TITLE_RE.search(normalize_title(title)))
    years = required_years(text, strict_cues=new_grad_title)
    if years is not None:
        if years >= 3:
            return True
        if years == 2 and not new_grad_title:
            return True
    return any(
        asks_for_seniority(sentence) and not optional and not PREFERRED_CUE_RE.search(sentence)
        for sentence, optional in requirement_sentences(text)
    )


def asks_for_seniority(sentence: str) -> bool:
    match = SENIOR_PHRASE_RE.search(sentence)
    if not match or sentence.rstrip().endswith("?"):  # "How many years ... do you have?"
        return False
    return not NEGATED_BEFORE_RE.search(sentence[max(0, match.start() - 40): match.start()])


def matches(job: dict) -> bool:
    return rejection_reason(job) == "matched"


def rejection_reason(job: dict, now: int | None = None) -> str:
    if not job.get("active") or not job.get("is_visible", True):
        return "inactive"
    posted_at = freshness_timestamp(job)
    if posted_at is None:
        return "missing_posted_time"
    current_time = int(time.time()) if now is None else now
    age = current_time - posted_at
    if age < -FUTURE_TIMESTAMP_GRACE_SECONDS or age > MAX_JOB_AGE_SECONDS:
        return "outside_24_hours"
    if is_excluded_company(job):
        return "excluded_company"
    title = str(job.get("title", ""))
    title_reason = title_rejection(title)
    if title_reason:
        return title_reason
    description = str(job.get("description", ""))
    if asks_for_too_much_experience(f"{title}. {description}", title):
        return "too_much_experience"
    if not is_us_location(job.get("locations") or [], description):
        return "outside_us"
    if not description_mentions_citizenship_requirement(description):
        return "citizenship_requirement"
    if FILTER_NO_SPONSORSHIP and refuses_sponsorship(description):
        return "no_sponsorship"
    return "matched"


def stable_id(job: dict) -> str:
    value = str(job.get("id") or job.get("url") or json.dumps(job, sort_keys=True))
    return hashlib.sha256(value.encode()).hexdigest()


TRACKING_PARAMS = {"ref", "gh_src", "source", "src", "lever-source", "lever-origin"}


def canonical_url(job: dict) -> str:
    raw_url = str(job.get("url", ""))
    if not raw_url:
        return ""
    parsed = urllib.parse.urlsplit(raw_url)
    path = parsed.path.rstrip("/")
    if path.endswith("/application"):
        path = path.removesuffix("/application")
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [
        (key, value) for key, value in query
        if not key.lower().startswith("utm_") and key.lower() not in TRACKING_PARAMS
    ]
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), path, urllib.parse.urlencode(query), "")
    )


def job_fingerprint(job: dict) -> str:
    fields = [
        str(job.get("company_name", "")),
        str(job.get("title", "")),
        *sorted(str(location) for location in job.get("locations") or []),
    ]
    normalized = "|".join(re.sub(r"[^a-z0-9]+", " ", field.lower()).strip() for field in fields)
    return hashlib.sha256(normalized.encode()).hexdigest()


def connect_db() -> sqlite3.Connection:
    STATE_DIR.mkdir(exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS seen_jobs "
        "(id TEXT PRIMARY KEY, first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS seen_urls "
        "(url TEXT PRIMARY KEY, first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS initialized_boards "
        "(board_key TEXT PRIMARY KEY, initialized_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS seen_fingerprints "
        "(fingerprint TEXT PRIMARY KEY, first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS board_stats ("
        "board_key TEXT PRIMARY KEY, last_checked INTEGER NOT NULL, "
        "relevant_count INTEGER NOT NULL DEFAULT 0, match_count INTEGER NOT NULL DEFAULT 0, "
        "failure_count INTEGER NOT NULL DEFAULT 0)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS source_checks "
        "(source TEXT PRIMARY KEY, last_checked INTEGER NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS heartbeat "
        "(id INTEGER PRIMARY KEY CHECK (id = 1), last_run INTEGER NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS run_metrics ("
        "run_at INTEGER NOT NULL, source TEXT NOT NULL, fetched INTEGER NOT NULL, "
        "matched INTEGER NOT NULL, new_jobs INTEGER NOT NULL, failures INTEGER NOT NULL, "
        "rejections TEXT NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS http_cache (url TEXT PRIMARY KEY, etag TEXT)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS board_etags (board_key TEXT PRIMARY KEY, etag TEXT)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS observed_source_jobs ("
        "source TEXT NOT NULL, job_id TEXT NOT NULL, "
        "first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        "PRIMARY KEY(source, job_id))"
    )
    return connection


def loose_fingerprint(job: dict) -> str:
    company = normalize_company(job.get("company_name"))
    title = re.sub(r"[^a-z0-9]+", " ", str(job.get("title", "")).lower()).strip()
    return hashlib.sha256(f"loose|{company}|{title}".encode()).hexdigest()


def job_seen(connection: sqlite3.Connection, job: dict) -> bool:
    if connection.execute(
        "SELECT 1 FROM seen_jobs WHERE id = ?", (stable_id(job),)
    ).fetchone():
        return True
    if connection.execute(
        "SELECT 1 FROM seen_fingerprints WHERE fingerprint IN (?, ?)",
        (job_fingerprint(job), loose_fingerprint(job)),
    ).fetchone():
        return True
    url = canonical_url(job)
    return bool(url and connection.execute(
        "SELECT 1 FROM seen_urls WHERE url = ?", (url,)
    ).fetchone())


def record_jobs(connection: sqlite3.Connection, jobs: list[dict]) -> None:
    connection.executemany(
        "INSERT OR IGNORE INTO seen_jobs(id) VALUES (?)",
        [(stable_id(job),) for job in jobs],
    )
    connection.executemany(
        "INSERT OR IGNORE INTO seen_urls(url) VALUES (?)",
        [(url,) for job in jobs if (url := canonical_url(job))],
    )
    connection.executemany(
        "INSERT OR IGNORE INTO seen_fingerprints(fingerprint) VALUES (?)",
        [(fingerprint,) for job in jobs for fingerprint in (job_fingerprint(job), loose_fingerprint(job))],
    )


def load_boards() -> list[dict]:
    path = BOARDS_PATH if BOARDS_PATH.exists() else BOOTSTRAP_BOARDS_PATH
    boards = json.loads(path.read_text()) if path.exists() else []
    denied = load_board_denylist()
    return [
        board for board in boards
        if board.get("provider") in SUPPORTED_PROVIDERS and registry_key(board) not in denied
    ]


def load_feeds() -> list[dict]:
    path = FEEDS_PATH if FEEDS_PATH.exists() else BOOTSTRAP_FEEDS_PATH
    feeds = json.loads(path.read_text()) if path.exists() else []
    return [
        feed for feed in feeds
        if feed.get("name") and feed.get("url") and feed.get("approved") is True
    ]


def board_interval_seconds(relevant_count: int, match_count: int, provider: str = "") -> int:
    # Unchanged boards answer 304 (or a tiny Workday page), so polling is cheap;
    # boards with roles in our area are checked on every run.
    if match_count > 0 or relevant_count > 0:
        return HOT_POLL_SECONDS
    return PROVIDER_BASE_POLL_SECONDS.get(provider, BASE_POLL_SECONDS)


def jittered(seconds: int, board_key: str) -> int:
    """Spread boards over +-25% of their interval (stable per board) so they don't
    all come due on the same run."""
    fraction = int(hashlib.sha256(board_key.encode()).hexdigest()[:4], 16) / 0xFFFF
    return int(seconds * (0.75 + 0.5 * fraction))


FAILURE_PARK_COUNT = 10
FAILURE_PARK_SECONDS = 7 * 24 * 60 * 60


def failure_retry_seconds(failure_count: int) -> int:
    """Retry a failing board after 5 min, 10 min, 20 min and so on, up to a day. Only a
    board that has failed for about 42 hours straight is moved to weekly checks. (It
    used to take 3 failures, so Workday's hour of maintenance pages on 2026-11-21
    benched 1,273 Workday boards for a week.)"""
    if failure_count >= FAILURE_PARK_COUNT:
        return FAILURE_PARK_SECONDS
    return min(5 * 60 * (2 ** max(failure_count - 1, 0)), 24 * 60 * 60)

def select_due_boards(
    connection: sqlite3.Connection,
    boards: list[dict],
    now: int | None = None,
) -> list[dict]:
    current_time = int(time.time()) if now is None else now
    stats = {
        row[0]: {
            "last_checked": row[1],
            "relevant_count": row[2],
            "match_count": row[3],
            "failure_count": row[4],
        }
        for row in connection.execute(
            "SELECT board_key, last_checked, relevant_count, match_count, failure_count "
            "FROM board_stats"
        )
    }
    due = []
    for board in boards:
        board_key = f'{board["provider"]}:{board["slug"]}'
        board_stats = stats.get(board_key)
        if board_stats is None:
            next_check = 0
        elif board_stats["failure_count"]:
            next_check = board_stats["last_checked"] + failure_retry_seconds(
                board_stats["failure_count"]
            )
        else:
            next_check = board_stats["last_checked"] + jittered(board_interval_seconds(
                board_stats["relevant_count"], board_stats["match_count"], board["provider"]
            ), board_key)
        if next_check <= current_time:
            due.append((next_check, board_key, board))
    due.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in due[:ATS_BATCH_SIZE]]


def record_board_success(
    connection: sqlite3.Connection,
    board: dict,
    relevant_count: int,
    match_count: int,
    checked_at: int,
) -> None:
    board_key = f'{board["provider"]}:{board["slug"]}'
    connection.execute(
        "INSERT INTO board_stats(board_key, last_checked, relevant_count, match_count, failure_count) "
        "VALUES (?, ?, ?, ?, 0) ON CONFLICT(board_key) DO UPDATE SET "
        "last_checked=excluded.last_checked, relevant_count=excluded.relevant_count, "
        "match_count=excluded.match_count, failure_count=0",
        (board_key, checked_at, relevant_count, match_count),
    )


def record_board_failure(
    connection: sqlite3.Connection, board: dict, checked_at: int
) -> None:
    board_key = f'{board["provider"]}:{board["slug"]}'
    connection.execute(
        "INSERT INTO board_stats(board_key, last_checked, failure_count) VALUES (?, ?, 1) "
        "ON CONFLICT(board_key) DO UPDATE SET "
        "last_checked=excluded.last_checked, failure_count=failure_count+1",
        (board_key, checked_at),
    )


def record_board_unreachable(
    connection: sqlite3.Connection, board: dict, checked_at: int
) -> None:
    """Count a lone network error as a check that found nothing, not a board failure."""
    board_key = f'{board["provider"]}:{board["slug"]}'
    connection.execute(
        "INSERT INTO board_stats(board_key, last_checked) VALUES (?, ?) "
        "ON CONFLICT(board_key) DO UPDATE SET last_checked=excluded.last_checked",
        (board_key, checked_at),
    )


def is_network_error(exc: BaseException) -> bool:
    """True for errors that say nothing about the board: DNS, resets, TLS, timeouts."""
    if isinstance(exc, urllib.error.HTTPError):
        return False
    return isinstance(exc, (
        urllib.error.URLError,
        socket.timeout,
        TimeoutError,
        ConnectionError,
        ssl.SSLError,
        http.client.IncompleteRead,
    ))


TRANSIENT_HTTP_STATUSES = {429, 502, 503, 504}


def is_transient_http_error(exc: BaseException) -> bool:
    """Throttling or a gateway hiccup: retry later without counting it against the board."""
    return isinstance(exc, urllib.error.HTTPError) and exc.code in TRANSIENT_HTTP_STATUSES


def is_missing_board_error(exc: BaseException) -> bool:
    return isinstance(exc, urllib.error.HTTPError) and exc.code in {404, 410}


def network_outage(success_count: int, unreachable_count: int) -> bool:
    """A batch where network errors outnumber successes means we were offline."""
    if unreachable_count == 0:
        return False
    return success_count == 0 or unreachable_count >= max(3, success_count)


def network_available(hosts: tuple[str, ...] = NETWORK_CHECK_HOSTS) -> bool:
    for host in hosts:
        try:
            socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        except OSError:
            continue
        return True
    return False


def wait_for_network(
    hosts: tuple[str, ...] = NETWORK_CHECK_HOSTS,
    timeout: float = NETWORK_WAIT_SECONDS,
    interval: float = NETWORK_RETRY_SECONDS,
    sleep=time.sleep,
    clock=time.monotonic,
) -> bool:
    deadline = clock() + timeout
    while not network_available(hosts):
        if clock() >= deadline:
            return False
        sleep(interval)
    return True


# Block-level tags end a line, so each bullet of a requirements list stays its own
# sentence. Flattened into one line, a "nice to have" in one bullet used to cancel
# the "5+ years" in the bullet next to it.
BLOCK_TAG_RE = re.compile(r"<\s*(?:br|li|/li|/p|/div|/h[1-6]|/tr|/ul|/ol|/section)\b[^>]*>", re.I)


def plain_text(value: str) -> str:
    decoded = html.unescape(html.unescape(value or ""))
    text = re.sub(r"<[^>]+>", " ", BLOCK_TAG_RE.sub("\n", decoded))
    text = re.sub(r"[^\S\n]+", " ", text)
    return re.sub(r"\s*\n\s*", "\n", text).strip()


def parse_job_datetime(value: object) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value / 1000 if value > 10**12 else value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        numeric = int(text)
        return int(numeric / 1000 if numeric > 10**12 else numeric)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    elif text.endswith((" UTC", " GMT")):  # Recruitee: "2026-11-19 16:00:31 UTC"
        text = f"{text[:-4]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def experience_summary(job: dict) -> str:
    title = str(job.get("title", "")).lower()
    description = str(job.get("description", "")).lower()
    text = f"{title} {description}"
    if any(term in text for term in ("new grad", "new graduate", "graduate", "entry level", "entry-level", "early career", "campus hire")):
        return "New grad / entry level"
    match = re.search(
        r"\b(?:0\s*(?:-|to)\s*2|1\s*(?:-|to)\s*2|2\s*(?:-|to)\s*4)\s*years?\b",
        text,
    )
    if match:
        return match.group(0).replace("  ", " ")
    if "junior" in text or "associate" in text or "i" in title.split():
        return "Junior / entry level"
    return "Entry level"


try:
    from zoneinfo import ZoneInfo
    DISPLAY_TZ = ZoneInfo("America/New_York")
except Exception:  # no tz database: fall back to UTC
    DISPLAY_TZ = timezone.utc


def posted_label(job: dict, now: int | None = None) -> str:
    """When the company posted the job (not when the watcher found it)."""
    current_time = int(time.time()) if now is None else now
    precision = job.get("posted_precision", "time")
    timestamp = job_posted_timestamp(job)
    if timestamp is None:
        detected = parse_job_datetime(job.get("detected_at"))
        seen = f" (seen {_ago(current_time - detected)})" if detected else ""
        return f"Not published{seen}"
    if precision == "date":
        day = datetime.fromtimestamp(timestamp, timezone.utc)
        return f"{day:%b} {day.day}"
    local = datetime.fromtimestamp(timestamp, DISPLAY_TZ)
    zone = "ET" if DISPLAY_TZ is not timezone.utc else "UTC"
    stamp = f"{local:%b} {local.day}, {local.hour % 12 or 12}:{local:%M} {local:%p} {zone}"
    if precision == "listed":
        return f"Listed {_ago(current_time - timestamp)} on Simplify ({stamp})"
    return f"{_ago(current_time - timestamp)} · {stamp}"


def _ago(seconds: int) -> str:
    seconds = max(int(seconds), 0)
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 172800:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def relative_posted_time(job: dict, now: int | None = None) -> str:
    if job.get("posted_display"):
        return str(job["posted_display"])
    current_time = int(time.time()) if now is None else now
    posted_at = job_posted_timestamp(job)
    if posted_at is None:
        return "Unknown"
    age = max(current_time - posted_at, 0)
    if age < 60:
        return "Just now"
    minutes = age // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    return f"{hours}h ago"


def role_category(job: dict) -> str:
    title = str(job.get("title", "")).lower()
    description = str(job.get("description", "")).lower()
    text = f"{title} {description}"
    categories = [
        ("robotics", (
            "robotics", "perception engineer", "autonomy", "control systems",
        )),
        ("ml", (
            "machine learning engineer", "ml engineer", "mlops", "ml ops",
            "ml platform", "ml infrastructure", "ml research",
        )),
        ("ai", (
            "ai engineer", "artificial intelligence", "genai", "gen ai",
            "llm", "inference engineer", "nlp engineer", "prompt", "rag",
        )),
        ("data_science", (
            "data scientist", "decision scientist", "applied scientist",
            "research scientist", "scientist", "experiment", "ab test",
        )),
        ("data", (
            "data engineer", "analytics engineer", "data analyst",
            "business intelligence", "bi engineer", "analytics",
        )),
        ("sde", (
            "software engineer", "software developer", "backend engineer",
            "back-end engineer", "frontend engineer", "front-end engineer",
            "full stack engineer", "full-stack engineer", "platform engineer",
            "infra engineer", "infrastructure engineer", "devops engineer",
            "site reliability engineer", "distributed systems engineer",
            "security engineer", "qa automation engineer", "sdet",
        )),
    ]
    for label, terms in categories:
        if any(term in text for term in terms):
            return label
    return "other"


def job_posted_timestamp(job: dict) -> int | None:
    for key in (
        "posted_at",
        "published_at",
        "publishedAt",
        "updated_at",
        "updatedAt",
        "created_at",
        "createdAt",
        "date_posted",
        "datePosted",
        "released_at",
        "releasedAt",
        "created",
        "releasedDate",
        "releaseDate",
        "postedDate",
    ):
        timestamp = parse_job_datetime(job.get(key))
        if timestamp is not None:
            return timestamp
    return None


def freshness_timestamp(job: dict) -> int | None:
    """When the job became visible: first-seen time for date-only or undated
    postings, otherwise the posting time."""
    return parse_job_datetime(job.get("detected_at")) or job_posted_timestamp(job)


def job_is_recent(job: dict, now: int | None = None) -> bool:
    current_time = int(time.time()) if now is None else now
    posted_at = freshness_timestamp(job)
    if posted_at is None:
        return False
    age = current_time - posted_at
    return -FUTURE_TIMESTAMP_GRACE_SECONDS <= age <= MAX_JOB_AGE_SECONDS


def source_timestamp_is_recent(value: object, now: int | None = None) -> bool:
    current_time = int(time.time()) if now is None else now
    timestamp = parse_job_datetime(value)
    if timestamp is None:
        return False
    age = current_time - timestamp
    return -FUTURE_TIMESTAMP_GRACE_SECONDS <= age <= MAX_JOB_AGE_SECONDS


def company_from_slug(slug: str) -> str:
    return re.sub(r"[-_]+", " ", slug).title()


def direct_job(
    board: dict,
    job_id: object,
    title: str,
    locations: list[str],
    url: str,
    description: str,
    posted_at: object | None = None,
) -> dict:
    board_key = f'{board["provider"]}:{board["slug"]}'
    return {
        "source": "direct-ats",
        "board_key": board_key,
        "id": f"{board_key}:{job_id}",
        "title": title,
        "company_name": board_company(board),
        "locations": [location for location in locations if location],
        "url": url,
        "description": description,
        "posted_at": posted_at,
        "active": True,
        "is_visible": True,
    }


class NotModified(Exception):
    """The board's list endpoint answered 304: nothing changed since the last poll."""


def fetch_json(url: str, data: bytes | None = None, board: dict | None = None) -> object:
    """GET (or POST) JSON with gzip. With a board, send its ETag and remember the new one."""
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": "personal-new-grad-job-watcher/1.0",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    etag = board.get("_etag") if board is not None else None
    if etag:
        headers["If-None-Match"] = etag
    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            if board is not None and response.headers.get("ETag"):
                board["_new_etag"] = response.headers["ETag"]
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        if exc.code == 304 and etag:
            raise NotModified(url) from None
        raise


def fetch_direct_board(board: dict) -> tuple[dict, list[dict]]:
    try:
        return board, read_board(board)
    except NotModified:
        board["_not_modified"] = True
        return board, []


def read_board(board: dict) -> list[dict]:
    provider = board["provider"]
    if provider in CAREER_SITE_FETCHERS:
        return CAREER_SITE_FETCHERS[provider](board, int(time.time()))
    slug = urllib.parse.quote(board["slug"], safe="")
    normalized = []
    if provider in {"lever", "lever-eu"}:
        domain = "api.eu.lever.co" if provider == "lever-eu" else "api.lever.co"
        payload = fetch_json(f"https://{domain}/v0/postings/{slug}?mode=json", board=board)
        for item in payload:
            posted_at = item.get("createdAt") or item.get("created_at") or item.get("updatedAt")
            if not source_timestamp_is_recent(posted_at):
                continue
            categories = item.get("categories") or {}
            locations = categories.get("allLocations") or [categories.get("location", "")]
            if item.get("country") == "US":
                locations = [*locations, "United States"]
            # Requirements ("Qualifications", "What you'll need") live in `lists`,
            # not in descriptionPlain.
            sections = [
                f'{section.get("text", "")}\n{plain_text(str(section.get("content", "")))}'
                for section in item.get("lists") or [] if isinstance(section, dict)
            ]
            description = "\n".join(filter(None, [
                str(item.get("openingPlain", "")),
                str(item.get("descriptionPlain", "")),
                *sections,
                str(item.get("additionalPlain", "")),
            ]))
            normalized.append(direct_job(
                board,
                item.get("id"),
                str(item.get("text", "")),
                [str(location) for location in locations],
                str(item.get("hostedUrl", "")),
                description,
                posted_at,
            ))
    elif provider == "ashby":
        payload = fetch_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}", board=board)
        for item in payload.get("jobs", []):
            if not item.get("isListed", True):
                continue
            posted_at = item.get("publishedAt") or item.get("published_at")
            if not source_timestamp_is_recent(posted_at):
                continue
            secondary = [
                str(location.get("location", ""))
                for location in item.get("secondaryLocations") or []
            ]
            addresses = [item.get("address") or {}] + [
                location.get("address") or {}
                for location in item.get("secondaryLocations") or []
            ]
            countries = [
                str((address.get("postalAddress") or address).get("addressCountry", ""))
                for address in addresses
            ]
            url = str(item.get("jobUrl") or item.get("applyUrl") or "")
            job_id = urllib.parse.urlsplit(url).path.rstrip("/").split("/")[-1]
            normalized.append(direct_job(
                board,
                job_id,
                str(item.get("title", "")),
                [str(item.get("location", "")), *secondary, *countries],
                url,
                str(item.get("descriptionPlain", "")),
                posted_at,
            ))
    elif provider == "recruitee":
        payload = fetch_json(f"https://{slug}.recruitee.com/api/offers/")
        items = payload.get("offers", payload if isinstance(payload, list) else [])
        for item in items:
            location = item.get("location") or {}
            if isinstance(location, dict):
                location = location.get("name") or location.get("city") or ""
            locations = [str(location)]
            locations.extend(
                str(value.get("name") if isinstance(value, dict) else value)
                for value in item.get("locations") or []
            )
            if item.get("remote"):
                locations.append("Remote")
            normalized.append(direct_job(
                board,
                item.get("id") or item.get("slug"),
                str(item.get("title", "")),
                locations,
                str(item.get("careers_url") or item.get("url") or ""),
                plain_text(str(item.get("description") or item.get("description_html") or "")),
                item.get("published_at") or item.get("updated_at") or item.get("created_at"),
            ))
    elif provider == "workable":
        payload = fetch_json(
            f"https://www.workable.com/api/accounts/{slug}?details=true", board=board
        )
        for item in payload.get("jobs", []):
            locations = []
            for location in item.get("locations") or []:
                if isinstance(location, dict):
                    locations.append(", ".join(filter(None, [
                        str(location.get("city", "")),
                        str(location.get("region", "")),
                        str(location.get("country", "")),
                    ])))
            locations.append(", ".join(filter(None, [
                str(item.get("city", "")),
                str(item.get("state", "")),
                str(item.get("country", "")),
            ])))
            if item.get("telecommuting") and str(item.get("country", "")).lower() in {
                "us", "usa", "united states"
            }:
                locations.append("Remote in USA")
            description = " ".join(filter(None, [
                str(item.get("experience", "")),
                str(item.get("education", "")),
                plain_text(str(item.get("description", ""))),
            ]))
            job = direct_job(
                board,
                item.get("shortcode") or item.get("id"),
                str(item.get("title", "")),
                locations,
                str(item.get("url") or item.get("shortlink") or ""),
                description,
                item.get("published_on") or item.get("created_at"),
            )
            job["company_name"] = str(payload.get("name") or company_from_slug(board["slug"]))
            normalized.append(job)
    return normalized


# --- Career-site readers ----------------------------------------------------------
# Workday, Oracle Recruiting, BambooHR, Rippling, JazzHR and Jobvite. Each reads the
# same unauthenticated JSON, XML or HTML that the company's public careers page
# loads. Readers marked DETAIL list cheaply, then fetch the full posting only for job
# IDs they have not seen before and whose title passes the filter. The first poll of
# a board just records the IDs that exist (the silent baseline) without fetching any
# details.

DETAIL_PROVIDERS = {
    "greenhouse", "smartrecruiters", "workday", "oracle", "bamboohr", "rippling", "jobvite",
}
MAX_DETAIL_FETCHES_PER_BOARD = 25
WORKDAY_PAGE_SIZE = 20
WORKDAY_MAX_PAGES = 5
FRESH_DATE_SECONDS = 2 * 24 * 60 * 60


def board_company(board: dict) -> str:
    if board.get("company"):
        return str(board["company"])
    slug = str(board["slug"])
    if "/" in slug:
        parts = slug.split("/")
        slug = parts[1] if board.get("provider") == "workday" and len(parts) > 2 else parts[0].split(".")[0]
    return company_from_slug(slug)


DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def with_posting(job: dict, value: object, now: int) -> dict:
    """Keep the source's real posting time for display. When the source gives only a
    date (or nothing), freshness comes from when we first saw the job: a job first
    seen within two days of its posting date was just posted."""
    timestamp = parse_job_datetime(value)
    job["posted_at"] = value if timestamp is not None else None
    if timestamp is None:
        job["posted_precision"] = "unknown"
        job["detected_at"] = now
    elif DATE_ONLY_RE.match(str(value).strip()):
        job["posted_precision"] = "date"
        if now - timestamp <= FRESH_DATE_SECONDS:
            job["detected_at"] = now
    else:
        job["posted_precision"] = "time"
    return job


def workday_posted_days(text: object) -> int:
    value = str(text or "").lower()
    if "today" in value or "just" in value:
        return 0
    if "yesterday" in value:
        return 1
    match = re.search(r"(\d+)", value)
    return int(match.group(1)) if match else 99


class DetailPlan:
    """Bookkeeping for one board: which IDs were handled and which need details."""

    def __init__(self, board: dict):
        self.board = board
        self.known = board.get("_known_ids") or frozenset()
        self.baseline = bool(board.get("_baseline"))
        self.handled: list[str] = []
        self.fresh: list[tuple] = []

    def consider(self, job_id: object, title: object, recent: bool = True, payload=None) -> None:
        job_id = str(job_id)
        if not job_id or job_id in self.known:
            if job_id:
                self.handled.append(job_id)
            return
        if self.baseline or not recent or title_rejection(title):
            self.handled.append(job_id)
            return
        self.fresh.append((job_id, payload))

    def details(self, fetch_one) -> list[dict]:
        jobs = []
        for job_id, payload in self.fresh[:MAX_DETAIL_FETCHES_PER_BOARD]:
            try:
                job = fetch_one(job_id, payload)
            except Exception as exc:
                print(f"Detail fetch failed board={self.board['provider']}:{self.board['slug']} "
                      f"id={job_id} error={exc}", file=sys.stderr)
                continue  # left unhandled, so it is retried next run
            self.handled.append(job_id)
            if job:
                jobs.append(job)
        self.board["_listed_ids"] = self.handled
        unprocessed = {job_id for job_id, _ in self.fresh} - set(self.handled)
        if unprocessed:
            self.board.pop("_new_etag", None)  # poll the full list again next run
        return jobs


WORKDAY_WORKERS = 4  # Workday runs in its own pool, so pacing never blocks other boards
WORKDAY_HOST_SPACING_SECONDS = 0.35
WORKDAY_MAX_WAIT_SECONDS = 90  # per-run budget per host; later boards wait for the next run
_workday_lock = threading.Lock()
_workday_next_slot: dict[str, float] = {}
_workday_throttled: set[str] = set()


def workday_fetch(url: str, data: bytes | None = None) -> object:
    """Pace requests per Workday host (tenants share hosts; wd3 throttles hardest).
    After a 429, or when the host is backed up, the board is deferred (transient)
    and polled on a later run instead of waiting."""
    host = urllib.parse.urlsplit(url).netloc.split(".", 1)[-1]
    with _workday_lock:
        spacing = WORKDAY_HOST_SPACING_SECONDS
        now = time.monotonic()
        start = max(now, _workday_next_slot.get(host, 0.0))
        if host in _workday_throttled or start - now > WORKDAY_MAX_WAIT_SECONDS:
            # This host is backed up for this run; try the board again on a later run.
            raise urllib.error.HTTPError(url, 429, "Workday host busy this run", None, None)
        _workday_next_slot[host] = start + spacing
    if start > now:
        time.sleep(start - now)
    try:
        return fetch_json(url, data)
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            with _workday_lock:
                _workday_throttled.add(host)
        raise
    except ValueError:
        # The CXS API answers JSON, or a JSON error for a dead tenant (404, 422).
        # A 200 that is not JSON is Workday's maintenance page, as across wd1, wd3
        # and wd5 on 2026-11-21: retry later, without counting it against the board.
        raise urllib.error.HTTPError(
            url, 503, "Workday answered with a non-JSON page (maintenance)", None, None
        ) from None


def fetch_workday_board(board: dict, now: int) -> list[dict]:
    host, tenant, site = str(board["slug"]).split("/", 2)
    base = f"https://{host}/wday/cxs/{tenant}/{site}"
    plan = DetailPlan(board)
    for page in range(WORKDAY_MAX_PAGES):
        payload = workday_fetch(f"{base}/jobs", json.dumps({
            "appliedFacets": {}, "limit": WORKDAY_PAGE_SIZE,
            "offset": page * WORKDAY_PAGE_SIZE, "searchText": "",
        }).encode())
        postings = payload.get("jobPostings") or []
        for posting in postings:
            path = str(posting.get("externalPath") or "")
            if path:
                plan.consider(
                    path.rstrip("/").rsplit("/", 1)[-1], posting.get("title"),
                    workday_posted_days(posting.get("postedOn")) <= 1, (path, posting),
                )
        ages = [workday_posted_days(posting.get("postedOn")) for posting in postings]
        if (not postings or min(ages) > 1 or plan.baseline
                or (page + 1) * WORKDAY_PAGE_SIZE >= int(payload.get("total") or 0)):
            break

    def one(job_id, payload):
        path, posting = payload
        info = (workday_fetch(base + path) or {}).get("jobPostingInfo") or {}
        locations = [info.get("location") or posting.get("locationsText")]
        locations += [str(value) for value in info.get("additionalLocations") or []]
        locations.append((info.get("country") or {}).get("descriptor"))
        return with_posting(direct_job(
            board, job_id, str(info.get("title") or posting.get("title") or ""),
            [str(value) for value in locations if value],
            str(info.get("externalUrl") or f"https://{host}/{site}{path}"),
            plain_text(str(info.get("jobDescription") or "")),
            None,
        ), info.get("startDate"), now)

    return plan.details(one)


def fetch_oracle_board(board: dict, now: int) -> list[dict]:
    host, site = str(board["slug"]).split("/", 1)
    api = f"https://{host}/hcmRestApi/resources/latest"
    payload = fetch_json(
        f"{api}/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations"
        f"&finder=findReqs;siteNumber={site},limit=50,sortBy=POSTING_DATES_DESC"
    )
    items = (payload.get("items") or [{}])[0].get("requisitionList") or []
    plan = DetailPlan(board)
    for item in items:
        posted = parse_job_datetime(item.get("PostedDate"))
        plan.consider(item.get("Id"), item.get("Title"),
                      posted is None or now - posted <= FRESH_DATE_SECONDS, item)

    def one(job_id, item):
        detail = (fetch_json(
            f"{api}/recruitingCEJobRequisitionDetails?expand=all&onlyData=true"
            f"&finder=ById;Id=%22{urllib.parse.quote(job_id)}%22,siteNumber={site}"
        ).get("items") or [{}])[0]
        country = str(item.get("PrimaryLocationCountry") or "").upper()
        locations = [item.get("PrimaryLocation"), "United States" if country == "US" else ""]
        locations += [
            value.get("Name") or value.get("LocationName") or ""
            for value in item.get("secondaryLocations") or [] if isinstance(value, dict)
        ]
        description = " ".join(plain_text(str(detail.get(key) or "")) for key in (
            "ExternalDescriptionStr", "ExternalResponsibilitiesStr", "ExternalQualificationsStr",
        ))
        return with_posting(direct_job(
            board, job_id, str(item.get("Title") or ""),
            [str(value) for value in locations if value],
            f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{job_id}",
            description or plain_text(str(item.get("ShortDescriptionStr") or "")),
            None,
        ), item.get("PostedDate"), now)

    return plan.details(one)


def fetch_bamboohr_board(board: dict, now: int) -> list[dict]:
    base = f'https://{board["slug"]}.bamboohr.com/careers'
    plan = DetailPlan(board)
    for item in fetch_json(f"{base}/list").get("result") or []:
        plan.consider(item.get("id"), item.get("jobOpeningName"), True, item)

    def one(job_id, item):
        opening = (fetch_json(f"{base}/{job_id}/detail").get("result") or {}).get("jobOpening") or {}
        location = opening.get("location") or {}
        ats = opening.get("atsLocation") or {}
        locations = [
            ", ".join(filter(None, [location.get("city"), location.get("state"), location.get("addressCountry")])),
            ", ".join(filter(None, [ats.get("city"), ats.get("state"), ats.get("country")])),
        ]
        if item.get("isRemote"):
            locations.append("Remote")
        return with_posting(direct_job(
            board, job_id, str(opening.get("jobOpeningName") or item.get("jobOpeningName") or ""),
            [value for value in locations if value],
            str(opening.get("jobOpeningShareUrl") or f"{base}/{job_id}"),
            " ".join(filter(None, [
                str(opening.get("minimumExperience") or ""),
                plain_text(str(opening.get("description") or "")),
            ])),
            None,
        ), opening.get("datePosted"), now)

    return plan.details(one)


def fetch_rippling_board(board: dict, now: int) -> list[dict]:
    base = f'https://api.rippling.com/platform/api/ats/v1/board/{urllib.parse.quote(str(board["slug"]))}/jobs'
    plan = DetailPlan(board)
    for item in fetch_json(base) or []:
        plan.consider(item.get("uuid"), item.get("name"), True, item)

    def one(job_id, item):
        detail = fetch_json(f"{base}/{job_id}") or {}
        description = detail.get("description") or ""
        if isinstance(description, dict):
            description = " ".join(str(value or "") for value in description.values())
        locations = [str(value) for value in detail.get("workLocations") or []]
        locations.append(str((item.get("workLocation") or {}).get("label") or ""))
        job = with_posting(direct_job(
            board, job_id, str(detail.get("name") or item.get("name") or ""),
            [value for value in locations if value],
            str(detail.get("url") or item.get("url") or ""),
            plain_text(str(description)),
            None,
        ), detail.get("createdOn"), now)
        if detail.get("companyName") and not board.get("company"):
            job["company_name"] = str(detail["companyName"])
        return job

    return plan.details(one)


JOBVITE_ROW_RE = re.compile(
    r'<a href="(/[^"/]+/job/([^"/?#]+))[^"]*"[^>]*>(.*?)</a>\s*</td>\s*'
    r'(?:<td class="jv-job-list-location">(.*?)</td>)?',
    re.S,
)


def fetch_jobvite_board(board: dict, now: int) -> list[dict]:
    slug = urllib.parse.quote(str(board["slug"]))
    page = fetch_text(f"https://jobs.jobvite.com/{slug}/jobs")
    plan = DetailPlan(board)
    for path, job_id, title, location in JOBVITE_ROW_RE.findall(page):
        plan.consider(job_id, plain_text(title), True, (path, plain_text(title), plain_text(location or "")))

    def one(job_id, payload):
        path, title, location = payload
        detail = fetch_text(f"https://jobs.jobvite.com{path}")
        match = re.search(r'<div class="jv-job-detail-description"[^>]*>(.*?)<div class="jv-job-detail-bottom', detail, re.S)
        return with_posting(direct_job(
            board, job_id, title, [location],
            f"https://jobs.jobvite.com{path}",
            plain_text(match.group(1) if match else detail),
            None,
        ), None, now)  # Jobvite publishes no date; first seen is the best signal

    return plan.details(one)


def fetch_jazzhr_board(board: dict, now: int) -> list[dict]:
    feed = fetch_text(f'https://app.jazz.co/feeds/export/jobs/{urllib.parse.quote(str(board["slug"]))}')
    root = ET.fromstring(feed.encode())
    company = (root.findtext("company") or "").strip()
    jobs = []
    for item in root.iter("job"):
        if (item.findtext("status") or "Open").strip().lower() != "open":
            continue
        job_id = (item.findtext("id") or "").strip()
        stamp = re.search(r"job_(\d{14})_", job_id)
        posted = (
            datetime.strptime(stamp.group(1), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
            if stamp else None
        )
        if not source_timestamp_is_recent(posted, now):
            continue
        location = ", ".join(filter(None, [
            (item.findtext(tag) or "").strip() for tag in ("city", "state", "country")
        ]))
        job = direct_job(
            board, job_id, (item.findtext("title") or "").strip(), [location],
            (item.findtext("url") or "").strip(),
            " ".join(filter(None, [
                (item.findtext("experience") or "").strip(),
                plain_text(item.findtext("description") or ""),
            ])),
            posted,
        )
        if company and not board.get("company"):
            job["company_name"] = company
        jobs.append(job)
    return jobs


def fetch_greenhouse_board(board: dict, now: int) -> list[dict]:
    base = f'https://boards-api.greenhouse.io/v1/boards/{urllib.parse.quote(str(board["slug"]), safe="")}/jobs'
    payload = fetch_json(base, board=board)
    plan = DetailPlan(board)
    for item in payload.get("jobs") or []:
        published = parse_job_datetime(item.get("first_published") or item.get("updated_at"))
        plan.consider(item.get("id"), item.get("title"),
                      published is not None and now - published <= MAX_JOB_AGE_SECONDS, item)

    def one(job_id, item):
        detail = fetch_json(f"{base}/{job_id}") or {}
        job = direct_job(
            board, job_id, str(detail.get("title") or item.get("title") or ""),
            [str((detail.get("location") or item.get("location") or {}).get("name", ""))],
            str(detail.get("absolute_url") or item.get("absolute_url") or ""),
            plain_text(str(detail.get("content") or "")),
            item.get("first_published") or detail.get("first_published") or item.get("updated_at"),
        )
        if item.get("company_name") and not board.get("company"):
            job["company_name"] = str(item["company_name"])
        return job

    return plan.details(one)


def fetch_smartrecruiters_board(board: dict, now: int) -> list[dict]:
    slug = urllib.parse.quote(str(board["slug"]), safe="")
    api = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    plan = DetailPlan(board)
    offset = 0
    while offset < 300:
        payload = fetch_json(f"{api}?limit=100&offset={offset}&country=us")
        items = payload.get("content") or []
        for item in items:
            posted = item.get("releasedDate") or item.get("releaseDate") or item.get("createdOn")
            plan.consider(item.get("id"), item.get("name"), source_timestamp_is_recent(posted, now), item)
        offset += len(items)
        if not items or plan.baseline or offset >= int(payload.get("totalFound") or 0):
            break

    def one(job_id, item):
        detail = fetch_json(f"{api}/{urllib.parse.quote(job_id, safe='')}") or {}
        sections = ((detail.get("jobAd") or {}).get("sections") or {}).values()
        experience = str((detail.get("experienceLevel") or {}).get("label", ""))
        location = detail.get("location") or item.get("location") or {}
        locations = [str(location.get("fullLocation", ""))]
        if str(location.get("country", "")).lower() == "us":
            locations.append("United States")
        job = direct_job(
            board, detail.get("id") or job_id, str(detail.get("name") or item.get("name") or ""),
            locations,
            str(detail.get("applyUrl") or f"https://jobs.smartrecruiters.com/{slug}/{job_id}"),
            " ".join([experience, *[plain_text(str(section.get("text", ""))) for section in sections]]),
            detail.get("releasedDate") or detail.get("releaseDate") or detail.get("createdOn")
            or item.get("releasedDate") or item.get("createdOn"),
        )
        job["company_name"] = str(
            (detail.get("company") or item.get("company") or {}).get("name") or board_company(board)
        )
        return job

    return plan.details(one)


CAREER_SITE_FETCHERS = {
    "greenhouse": fetch_greenhouse_board,
    "smartrecruiters": fetch_smartrecruiters_board,
    "workday": fetch_workday_board,
    "oracle": fetch_oracle_board,
    "bamboohr": fetch_bamboohr_board,
    "rippling": fetch_rippling_board,
    "jobvite": fetch_jobvite_board,
    "jazzhr": fetch_jazzhr_board,
}


def prepare_boards(connection: sqlite3.Connection, boards: list[dict]) -> list[dict]:
    """Attach ETags, and known job IDs plus baseline status for DetailPlan readers."""
    initialized = {row[0] for row in connection.execute("SELECT board_key FROM initialized_boards")}
    etags = dict(connection.execute("SELECT board_key, etag FROM board_etags"))
    prepared = []
    for board in boards:
        key = f'{board["provider"]}:{board["slug"]}'
        extra = {"_etag": etags.get(key)} if etags.get(key) and key in initialized else {}
        if board.get("provider") in DETAIL_PROVIDERS:
            extra["_known_ids"] = frozenset(row[0] for row in connection.execute(
                "SELECT job_id FROM observed_source_jobs WHERE source = ?", (key,)
            ))
            extra["_baseline"] = key not in initialized
        prepared.append({**board, **extra} if extra else board)
    return prepared


def record_board_checked(connection: sqlite3.Connection, board: dict, checked_at: int) -> None:
    """A 304: the board answered and nothing changed, so keep its counts."""
    connection.execute(
        "INSERT INTO board_stats(board_key, last_checked) VALUES (?, ?) "
        "ON CONFLICT(board_key) DO UPDATE SET last_checked=excluded.last_checked, failure_count=0",
        (f'{board["provider"]}:{board["slug"]}', checked_at),
    )


def remember_etag(connection: sqlite3.Connection, board: dict) -> None:
    if board.get("_new_etag"):
        connection.execute(
            "INSERT INTO board_etags(board_key, etag) VALUES (?, ?) "
            "ON CONFLICT(board_key) DO UPDATE SET etag=excluded.etag",
            (f'{board["provider"]}:{board["slug"]}', board["_new_etag"]),
        )


def remember_listed_ids(connection: sqlite3.Connection, board: dict) -> None:
    ids = board.get("_listed_ids")
    if ids:
        key = f'{board["provider"]}:{board["slug"]}'
        connection.executemany(
            "INSERT OR IGNORE INTO observed_source_jobs(source, job_id) VALUES (?, ?)",
            [(key, job_id) for job_id in ids],
        )


# --- Board discovery ------------------------------------------------------------------
# SimplifyJobs publishes its curated new-grad and internship lists as JSON. Every apply
# link names a company's ATS board, so the lists double as a directory of companies
# that hire new grads. New boards are added to state/ats_boards.json once a day, and
# fresh new-grad listings are also used as a backstop source for roles on ATSes the
# watcher can't read (iCIMS, custom career sites).

SIMPLIFY_LISTINGS = {
    "simplify-newgrad": "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json",
    "simplify-interns": "https://raw.githubusercontent.com/SimplifyJobs/Summer2027-Internships/dev/.github/scripts/listings.json",
}
SIMPLIFY_INTERVALS = {"simplify-newgrad": 15 * 60, "simplify-interns": 24 * 60 * 60}
HARVEST_CATEGORIES = {
    "Software", "AI/ML/Data", "Software Engineering", "Data Science, AI & Machine Learning",
}
HARVEST_MAX_AGE_SECONDS = 365 * 24 * 60 * 60
LOCALE_SEGMENT_RE = re.compile(r"^[a-z]{2}(?:-[A-Za-z]{2})?$")
SIMPLIFY_CITIZENSHIP = "U.S. Citizenship is Required"
SIMPLIFY_SPONSORSHIP_TEXT = {
    SIMPLIFY_CITIZENSHIP: "U.S. citizenship is required.",
    "Does Not Offer Sponsorship": "Visa sponsorship is not available.",
}


def board_from_url(url: str, company: str = "") -> dict | None:
    parts = urllib.parse.urlsplit(str(url or ""))
    host = parts.netloc.lower().split(":")[0]
    path = [segment for segment in parts.path.split("/") if segment]
    query = urllib.parse.parse_qs(parts.query)
    board = None
    if host.endswith("greenhouse.io"):
        slug = (query.get("for") or [None])[0] if path[:1] == ["embed"] else (path[0] if path else None)
        board = slug and {"provider": "greenhouse", "slug": slug.lower()}
    elif host in {"jobs.lever.co", "jobs.eu.lever.co"} and path:
        board = {"provider": "lever-eu" if ".eu." in host else "lever", "slug": path[0].lower()}
    elif host == "jobs.ashbyhq.com" and path:
        board = {"provider": "ashby", "slug": path[0]}
    elif host in {"jobs.smartrecruiters.com", "careers.smartrecruiters.com"} and path:
        board = {"provider": "smartrecruiters", "slug": path[0]}
    elif host == "apply.workable.com" and path and path[0] != "j":
        board = {"provider": "workable", "slug": path[0].lower()}
    elif host.endswith(".myworkdayjobs.com"):
        segments = [segment for segment in path if not LOCALE_SEGMENT_RE.match(segment)]
        if segments and segments[0] != "wday":
            board = {"provider": "workday", "slug": f"{host}/{host.split('.')[0]}/{segments[0]}"}
    elif host.endswith(".myworkdaysite.com") and "recruiting" in path:
        index = path.index("recruiting")
        if len(path) > index + 2:
            board = {"provider": "workday", "slug": f"{host}/{path[index + 1]}/{path[index + 2]}"}
    elif host.endswith("oraclecloud.com") and "sites" in path:
        index = path.index("sites")
        if len(path) > index + 1:
            board = {"provider": "oracle", "slug": f"{host}/{path[index + 1]}"}
    elif host.endswith(".bamboohr.com") and host.count(".") == 2:
        board = {"provider": "bamboohr", "slug": host.split(".")[0]}
    elif host.endswith(".applytojob.com"):
        board = {"provider": "jazzhr", "slug": host.split(".")[0]}
    elif host == "jobs.jobvite.com" and path:
        board = {"provider": "jobvite", "slug": path[0].lower()}
    elif host == "ats.rippling.com" and path:
        segments = [segment for segment in path if not LOCALE_SEGMENT_RE.match(segment)]
        board = segments and {"provider": "rippling", "slug": segments[0]}
    elif host.endswith(".recruitee.com"):
        board = {"provider": "recruitee", "slug": host.split(".")[0]}
    if not board:
        return None
    if company:
        board["company"] = company
    return board


def registry_key(board: dict) -> str:
    return f'{board.get("provider")}:{str(board.get("slug", "")).lower()}'


def write_registry(boards: list[dict], registry_path: Path) -> None:
    temporary = registry_path.with_name(f".{registry_path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(boards, indent=2) + "\n")
    os.replace(temporary, registry_path)


# Boards never to add back or migrate: slugs that belong to a different company
# elsewhere. Extend in state/board_denylist.json (a JSON list of provider:slug keys).
DEFAULT_BOARD_DENYLIST = ("greenhouse:alma", "ashby:alma")
BOARD_DENYLIST_PATH = STATE_DIR / "board_denylist.json"


def load_board_denylist() -> set[str]:
    keys = list(DEFAULT_BOARD_DENYLIST)
    try:
        if BOARD_DENYLIST_PATH.exists():
            keys += json.loads(BOARD_DENYLIST_PATH.read_text())
    except (OSError, ValueError) as exc:
        print(f"job-watcher: ignoring unreadable {BOARD_DENYLIST_PATH}: {exc}", file=sys.stderr)
    return {str(key).lower() for key in keys}


def harvest_boards(listings: list[dict], known_keys: set[str], now: int) -> list[dict]:
    found = []
    known_keys |= load_board_denylist()
    for listing in listings:
        if listing.get("category") not in HARVEST_CATEGORIES:
            continue
        if now - int(listing.get("date_posted") or 0) > HARVEST_MAX_AGE_SECONDS:
            continue
        company = str(listing.get("company_name") or "").strip()
        if normalize_company(company) in EXCLUDED_COMPANIES:
            continue
        board = board_from_url(listing.get("url", ""), company)
        if not board or board["provider"] not in SUPPORTED_PROVIDERS:
            continue
        key = registry_key(board)
        if key not in known_keys:
            known_keys.add(key)
            found.append({**board, "source": "simplify"})
    return found


def simplify_jobs(listings: list[dict], now: int) -> list[dict]:
    jobs = []
    for listing in listings:
        posted = int(listing.get("date_posted") or 0)
        if listing.get("category") not in HARVEST_CATEGORIES or now - posted > MAX_JOB_AGE_SECONDS:
            continue
        jobs.append({
            "source": "simplify",
            "id": f'simplify:{listing.get("id")}',
            "title": str(listing.get("title") or ""),
            "company_name": str(listing.get("company_name") or ""),
            "locations": [str(value) for value in listing.get("locations") or []],
            "url": str(listing.get("url") or ""),
            "description": SIMPLIFY_SPONSORSHIP_TEXT.get(str(listing.get("sponsorship") or ""), ""),
            "posted_at": posted,
            "posted_precision": "listed",
            "active": bool(listing.get("active", True)),
            "is_visible": bool(listing.get("is_visible", True)),
        })
    return jobs


SIMPLIFY_DUPLICATE_MARGIN_SECONDS = 24 * 60 * 60


def without_direct_duplicates(connection: sqlite3.Connection, jobs: list[dict]) -> list[dict]:
    """Drop Simplify copies of jobs on boards the watcher already reads in full.

    A Simplify listing has no description, so a copy of a posting that the direct
    reader rejected (5+ years, no sponsorship, clearance) would otherwise be mailed.
    A copy is dropped only when its board was baselined well before the listing
    (so the direct reader saw the job as new) and the board is reading cleanly.
    """
    baselined = {
        key.lower(): int(stamp or 0) for key, stamp in connection.execute(
            "SELECT board_key, strftime('%s', initialized_at) FROM initialized_boards"
        )
    }
    failing = {
        key.lower() for (key,) in connection.execute(
            "SELECT board_key FROM board_stats WHERE failure_count > 0"
        )
    }
    kept = []
    for job in jobs:
        board = board_from_url(job.get("url", ""))
        key = registry_key(board) if board else ""
        since = baselined.get(key)
        if (since is not None and key not in failing
                and since < int(job.get("posted_at") or 0) - SIMPLIFY_DUPLICATE_MARGIN_SECONDS):
            continue
        kept.append(job)
    return kept


# --- Posting pages for Simplify backstop jobs --------------------------------------
# Enabled on 2026-11-25 by the owner's decision (see SOURCE_POLICY.md). A Simplify
# listing carries no description, so a new backstop job that passes every other
# filter has its public posting page read once, with no login, cookies or scripts,
# and the experience, clearance and sponsorship rules run on that text. Most career
# sites (iCIMS, Workday, Jibe, Lever, Ashby) embed a schema.org JobPosting, which is
# the clean description; otherwise the page's visible text is used, cut off where
# the application form starts. A page that cannot be read leaves the job as it was.
SIMPLIFY_PAGE_READS_PER_RUN = 12
PAGE_READ_WORKERS = 4
PAGE_READ_TIMEOUT_SECONDS = 10
PAGE_READ_MAX_BYTES = 3_000_000
LD_JSON_RE = re.compile(r"<script[^>]*application/ld\+json[^>]*>(.*?)</script>", re.S | re.I)
HIDDEN_BLOCK_RE = re.compile(r"<(script|style|noscript|template|svg)\b.*?</\1\s*>", re.S | re.I)
APPLICATION_FORM_RE = re.compile(
    r"\b(?:apply for this (?:job|position|role)|submit (?:your )?application|first name\s*\*|"
    r"resume/cv|attach resume|upload (?:your )?resume|paste your resume|"
    r"voluntary self-identification)\b",
    re.I,
)


def job_postings(data: object):
    """Every schema.org JobPosting in a parsed JSON-LD block."""
    if isinstance(data, list):
        for item in data:
            yield from job_postings(item)
    elif isinstance(data, dict):
        kind = data.get("@type")
        if kind == "JobPosting" or (isinstance(kind, list) and "JobPosting" in kind):
            yield data
        for item in data.get("@graph") or []:
            yield from job_postings(item)


def posting_page_text(page: str) -> str:
    for block in LD_JSON_RE.findall(page):
        try:
            data = json.loads(block.strip())
        except ValueError:
            continue
        for posting in job_postings(data):
            parts = [str(posting.get(key) or "") for key in ("description", "qualifications")]
            experience = posting.get("experienceRequirements")
            if isinstance(experience, dict) and experience.get("monthsOfExperience"):
                months = int(float(experience["monthsOfExperience"]))
                parts.append(f"Minimum of {months // 12} years of experience required.")
            elif isinstance(experience, str):
                parts.append(experience)
            text = plain_text("\n".join(part for part in parts if part))
            if text:
                return text
    body = re.search(r"<body\b.*", page, re.S | re.I)
    text = plain_text(HIDDEN_BLOCK_RE.sub(" ", body.group(0) if body else page))
    form = APPLICATION_FORM_RE.search(text)
    return text[: form.start()] if form and form.start() > 500 else text


def ats_detail_text(url: str) -> str | None:
    """The description from the public detail endpoint the watcher's own reader uses,
    for posting pages that render only with JavaScript. None for other systems."""
    board = board_from_url(url)
    provider = (board or {}).get("provider")
    path = [segment for segment in urllib.parse.urlsplit(url).path.split("/") if segment]
    if provider == "workday" and "job" in path:
        host, tenant, site = str(board["slug"]).split("/", 2)
        rest = "/".join(path[path.index("job"):])
        info = (workday_fetch(f"https://{host}/wday/cxs/{tenant}/{site}/{rest}") or {}).get(
            "jobPostingInfo") or {}
        return plain_text(str(info.get("jobDescription") or ""))
    if provider == "oracle" and "job" in path and len(path) > path.index("job") + 1:
        host, site = str(board["slug"]).split("/", 1)
        job_id = urllib.parse.quote(path[path.index("job") + 1])
        detail = (fetch_json(
            f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
            f"?expand=all&onlyData=true&finder=ById;Id=%22{job_id}%22,siteNumber={site}"
        ).get("items") or [{}])[0]
        return "\n".join(plain_text(str(detail.get(key) or "")) for key in (
            "ExternalDescriptionStr", "ExternalResponsibilitiesStr", "ExternalQualificationsStr",
        )).strip()
    if provider == "bamboohr" and any(segment.isdigit() for segment in path):
        job_id = next(segment for segment in path if segment.isdigit())
        opening = (fetch_json(f'https://{board["slug"]}.bamboohr.com/careers/{job_id}/detail')
                   .get("result") or {}).get("jobOpening") or {}
        return "\n".join(filter(None, [
            str(opening.get("minimumExperience") or ""), plain_text(str(opening.get("description") or "")),
        ]))
    if provider == "workable" and "j" in path and len(path) > path.index("j") + 1:
        code = urllib.parse.quote(path[path.index("j") + 1])
        detail = fetch_json(f'https://apply.workable.com/api/v2/accounts/{board["slug"]}/jobs/{code}') or {}
        return "\n".join(plain_text(str(detail.get(key) or "")) for key in ("description", "requirements")).strip()
    return None


def fetch_posting_page(url: str) -> str:
    detail = ats_detail_text(url)
    if detail is not None:
        return detail
    request = urllib.request.Request(url, headers={
        "User-Agent": "personal-new-grad-job-watcher/1.0",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Encoding": "gzip",
    })
    with urllib.request.urlopen(request, timeout=PAGE_READ_TIMEOUT_SECONDS) as response:
        body = response.read(PAGE_READ_MAX_BYTES)
        if response.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        charset = response.headers.get_content_charset() or "utf-8"
    return posting_page_text(body.decode(charset, "replace"))


def with_page_descriptions(connection: sqlite3.Connection, jobs: list[dict]) -> list[dict]:
    """Fill in the description of new backstop jobs that would otherwise be mailed."""
    pending = [
        job for job in jobs
        if not job.get("description") and job.get("url") and matches(job)
        and not job_seen(connection, job)
    ][:SIMPLIFY_PAGE_READS_PER_RUN]
    failures = []

    def read(job: dict) -> None:
        try:
            text = fetch_posting_page(str(job["url"]))
        except Exception as exc:
            failures.append(job)
            print(f"Posting page read failed url={job['url']} error={exc}", file=sys.stderr)
            return
        if text:
            job["description"] = text

    if pending:
        with ThreadPoolExecutor(PAGE_READ_WORKERS) as executor:
            list(executor.map(read, pending))
        rejected = sum(1 for job in pending if not matches(job))
        print(f"Read {len(pending)} posting pages for Simplify jobs "
              f"({len(failures)} failed, {rejected} now rejected)")
    return jobs


def fetch_conditional(connection: sqlite3.Connection, url: str) -> bytes | None:
    """GET with If-None-Match and gzip; None when the file has not changed."""
    row = connection.execute("SELECT etag FROM http_cache WHERE url = ?", (url,)).fetchone()
    headers = {"User-Agent": "personal-new-grad-job-watcher/1.0", "Accept-Encoding": "gzip"}
    if row and row[0]:
        headers["If-None-Match"] = row[0]
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as response:
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            etag = response.headers.get("ETag")
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            return None
        raise
    connection.execute(
        "INSERT INTO http_cache(url, etag) VALUES (?, ?) "
        "ON CONFLICT(url) DO UPDATE SET etag=excluded.etag",
        (url, etag),
    )
    return body


def fetch_simplify(
    connection: sqlite3.Connection, boards: list[dict], now: int, registry_path: Path
) -> list[tuple[str, list[dict]]]:
    last_checks = dict(connection.execute("SELECT source, last_checked FROM source_checks"))
    known = {registry_key(board) for board in boards}
    known |= {str(board["migrated_from"]).lower() for board in boards if board.get("migrated_from")}
    new_boards: list[dict] = []
    results = []
    for name, url in SIMPLIFY_LISTINGS.items():
        if now - int(last_checks.get(name, 0)) < SIMPLIFY_INTERVALS[name]:
            continue
        try:
            body = fetch_conditional(connection, url)
            if body is not None:
                listings = json.loads(body)
                new_boards += harvest_boards(listings, known, now)
                if name == "simplify-newgrad":
                    results.append(("simplify", with_page_descriptions(
                        connection, without_direct_duplicates(connection, simplify_jobs(listings, now))
                    )))
        except Exception as exc:
            print(f"Simplify source failed ({name}): {exc}", file=sys.stderr)
        finally:
            connection.execute(
                "INSERT INTO source_checks(source, last_checked) VALUES (?, ?) "
                "ON CONFLICT(source) DO UPDATE SET last_checked=excluded.last_checked",
                (name, now),
            )
    if new_boards:
        source = registry_path if registry_path.exists() else BOOTSTRAP_BOARDS_PATH
        current = json.loads(source.read_text()) if source.exists() else []
        write_registry(current + new_boards, registry_path)
        counts = Counter(board["provider"] for board in new_boards)
        print(f"Harvested {len(new_boards)} new boards: " + ", ".join(
            f"{provider} {count}" for provider, count in counts.most_common()
        ))
    return results


def fetch_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/rss+xml, application/atom+xml, application/json, text/xml",
            "User-Agent": "personal-new-grad-job-watcher/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode(response.headers.get_content_charset() or "utf-8")


def fetch_feed_jobs(feed: dict) -> list[dict]:
    raw = fetch_text(str(feed["url"]))
    items = []
    if raw.lstrip().startswith(("{", "[")):
        payload = json.loads(raw)
        items = payload.get("items", payload if isinstance(payload, list) else [])
    else:
        root = ET.fromstring(raw)
        elements = (
            root.findall(".//item")
            or root.findall("{*}entry")
            or root.findall(".//job")
        )
        for element in elements:
            values = {}
            for child in element:
                key = child.tag.rsplit("}", 1)[-1]
                if key == "link" and not (child.text or "").strip():
                    values[key] = child.attrib.get("href", "")
                else:
                    values[key] = child.text or ""
            items.append(values)
    jobs = []
    default_location = "United States" if feed.get("us_only") is True else ""
    for item in items:
        url = item.get("url") or item.get("external_url") or item.get("link") or ""
        content = item.get("content_html") or item.get("content_text") or item.get("description") or item.get("summary") or ""
        location = item.get("location") or ", ".join(filter(None, [
            str(item.get("city", "")),
            str(item.get("state", "")),
            str(item.get("country", "")),
        ])) or default_location
        jobs.append(aggregator_job(
            f'feed:{feed["name"]}',
            item.get("id") or item.get("guid") or url,
            plain_text(str(item.get("title", ""))),
            str(item.get("company") or feed.get("company") or feed["name"]),
            str(location),
            str(url),
            plain_text(str(content)),
            item.get("date_published") or item.get("date_modified") or item.get("pubDate") or item.get("published") or item.get("updated") or item.get("date") or item.get("datePosted"),
            feed.get("us_only") is True,
        ))
    return jobs


def aggregator_job(
    source: str,
    job_id: object,
    title: str,
    company: str,
    location: str,
    url: str,
    description: str,
    posted_at: object | None = None,
    us_scope: bool = True,
) -> dict:
    return {
        "source": source,
        "board_key": f"aggregator:{source}",
        "id": f"{source}:{job_id}",
        "title": title,
        "company_name": company,
        "locations": [value for value in [location, "United States" if us_scope else ""] if value],
        "url": url,
        "description": description,
        "posted_at": posted_at,
        "active": True,
        "is_visible": True,
    }


def fetch_adzuna_jobs(app_id: str, app_key: str) -> list[dict]:
    query = urllib.parse.urlencode({
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 50,
        "what": "software engineer OR data engineer OR machine learning engineer OR AI engineer OR data scientist OR new grad OR entry level",
        "where": "United States",
        "sort_by": "date",
        "content-type": "application/json",
    })
    payload = fetch_json(f"https://api.adzuna.com/v1/api/jobs/us/search/1?{query}")
    jobs = []
    for item in payload.get("results", []):
        location = item.get("location") or {}
        company = item.get("company") or {}
        jobs.append(aggregator_job(
            "adzuna",
            item.get("id"),
            str(item.get("title", "")),
            str(company.get("display_name", "Unknown company")),
            str(location.get("display_name", "United States")),
            str(item.get("redirect_url", "")),
            str(item.get("description", "")),
            item.get("created"),
        ))
    return jobs


def fetch_jooble_jobs(api_key: str) -> list[dict]:
    body = json.dumps({
        "keywords": (
            "software engineer, data engineer, machine learning engineer, "
            "AI engineer, data scientist, new grad, entry level"
        ),
        "location": "United States",
        "page": "1",
        "ResultOnPage": "50",
        "companysearch": "false",
    }).encode()
    payload = fetch_json(
        f"https://jooble.org/api/{urllib.parse.quote(api_key, safe='')}", data=body
    )
    return [
        aggregator_job(
            "jooble",
            item.get("id"),
            str(item.get("title", "")),
            str(item.get("company", "Unknown company")),
            str(item.get("location", "United States")),
            str(item.get("link", "")),
            str(item.get("snippet", "")),
            item.get("updated") or item.get("created") or item.get("date"),
        )
        for item in payload.get("jobs", [])
    ]


def fetch_jobicy_jobs() -> list[dict]:
    payload = fetch_json("https://jobicy.com/api/v2/remote-jobs?count=100&geo=usa")
    return [
        aggregator_job(
            "jobicy",
            item.get("id") or item.get("jobSlug"),
            str(item.get("jobTitle", "")),
            str(item.get("companyName", "Unknown company")),
            str(item.get("jobGeo", "United States / Remote")),
            str(item.get("url", "")),
            plain_text(str(item.get("jobDescription") or item.get("jobExcerpt") or "")),
            item.get("pubDate") or item.get("datePosted"),
        )
        for item in payload.get("jobs", [])
    ]


def fetch_due_aggregators(
    connection: sqlite3.Connection, now: int
) -> list[tuple[str, list[dict]]]:
    configured = []
    configured.append(("jobicy", fetch_jobicy_jobs))
    adzuna_id = os.environ.get("JOB_WATCHER_ADZUNA_APP_ID")
    adzuna_key = os.environ.get("JOB_WATCHER_ADZUNA_APP_KEY")
    if adzuna_id and adzuna_key:
        configured.append(("adzuna", lambda: fetch_adzuna_jobs(adzuna_id, adzuna_key)))
    jooble_key = os.environ.get("JOB_WATCHER_JOOBLE_API_KEY")
    if jooble_key:
        configured.append(("jooble", lambda: fetch_jooble_jobs(jooble_key)))

    last_checks = dict(connection.execute("SELECT source, last_checked FROM source_checks"))
    results = []
    for source, fetcher in configured:
        interval = AGGREGATOR_INTERVALS.get(source, AGGREGATOR_INTERVAL_SECONDS)
        if now - int(last_checks.get(source, 0)) < interval:
            continue
        try:
            results.append((source, fetcher()))
        except Exception as exc:
            print(f"Aggregator source failed ({source}): {exc}", file=sys.stderr)
        finally:
            connection.execute(
                "INSERT INTO source_checks(source, last_checked) VALUES (?, ?) "
                "ON CONFLICT(source) DO UPDATE SET last_checked=excluded.last_checked",
                (source, now),
            )
    return results


def fetch_due_feeds(
    connection: sqlite3.Connection, feeds: list[dict], now: int
) -> tuple[list[tuple[str, list[dict]]], list[str]]:
    last_checks = dict(connection.execute("SELECT source, last_checked FROM source_checks"))
    results = []
    failures = []
    for feed in feeds:
        source = f'feed:{feed["name"]}'
        interval = max(
            int(feed.get("interval_seconds", FEED_INTERVAL_SECONDS)),
            FEED_INTERVAL_SECONDS,
        )
        if now - int(last_checks.get(source, 0)) < interval:
            continue
        try:
            results.append((source, fetch_feed_jobs(feed)))
        except Exception as exc:
            failures.append(source)
            print(f"Feed source failed ({source}): {exc}", file=sys.stderr)
        finally:
            connection.execute(
                "INSERT INTO source_checks(source, last_checked) VALUES (?, ?) "
                "ON CONFLICT(source) DO UPDATE SET last_checked=excluded.last_checked",
                (source, now),
            )
    return results, failures


def heartbeat_message(
    connection: sqlite3.Connection, now: int
) -> tuple[str, int | None]:
    row = connection.execute("SELECT last_run FROM heartbeat WHERE id = 1").fetchone()
    if not row:
        return "Heartbeat initialized", None
    last_run = int(row[0])
    gap = now - last_run
    if gap > HEARTBEAT_GRACE_SECONDS:
        return (
            f"Heartbeat gap detected: last run was {gap // 60} minutes ago "
            f"(expected every {HEARTBEAT_EXPECTED_SECONDS // 60} minutes)",
            last_run,
        )
    return (
        f"Heartbeat OK: last run was {max(gap // 60, 0)} minutes ago",
        last_run,
    )


def record_heartbeat(connection: sqlite3.Connection, now: int) -> None:
    connection.execute(
        "INSERT INTO heartbeat(id, last_run) VALUES (1, ?) "
        "ON CONFLICT(id) DO UPDATE SET last_run=excluded.last_run",
        (now,),
    )


def fetch_direct_batch(
    boards: list[dict],
) -> tuple[list[tuple[dict, list[dict]]], list[tuple[dict, Exception]]]:
    if not boards:
        return [], []
    results = []
    failures = []
    workday = [board for board in boards if board.get("provider") == "workday"]
    others = [board for board in boards if board.get("provider") != "workday"]
    with ThreadPoolExecutor(max_workers=8) as executor, \
            ThreadPoolExecutor(max_workers=WORKDAY_WORKERS) as workday_executor:
        futures = {executor.submit(fetch_direct_board, board): board for board in others}
        futures.update({
            workday_executor.submit(fetch_direct_board, board): board for board in workday
        })
        for future in as_completed(futures):
            board = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                failures.append((board, exc))
                print(
                    "ATS source failed "
                    f'provider={board.get("provider")} '
                    f'slug={board.get("slug")} '
                    f'url={board.get("url")} '
                    f"error={exc}",
                    file=sys.stderr,
                )
    return results, failures


def find_migrated_board(board: dict, fetch=None) -> dict | None:
    """Return the same slug on another ATS if it has listed jobs there."""
    fetch = fetch or fetch_json
    if "/" in str(board["slug"]):
        return None
    denied = load_board_denylist()
    if registry_key(board) in denied:
        return None
    old_key = f'{board["provider"]}:{board["slug"]}'
    family = "lever" if board["provider"].startswith("lever") else board["provider"]
    slug = urllib.parse.quote(board["slug"], safe="")
    for provider, template in MIGRATION_PROBE_URLS.items():
        if provider == family:
            continue
        try:
            payload = fetch(template.format(slug=slug))
        except Exception:
            continue
        if isinstance(payload, dict):
            jobs = payload.get("jobs") or []
        elif isinstance(payload, list):
            jobs = payload
        else:
            jobs = []
        if f'{provider}:{str(board["slug"]).lower()}' in denied:
            continue
        if any(isinstance(job, dict) and job.get("isListed", True) for job in jobs):
            return {"provider": provider, "slug": board["slug"], "migrated_from": old_key}
    return None


def find_board_migrations(boards: list[dict]) -> list[tuple[dict, dict | None]]:
    if not boards:
        return []
    with ThreadPoolExecutor(max_workers=4) as executor:
        return list(zip(boards, executor.map(find_migrated_board, boards)))


def save_board_migration(old_board: dict, new_board: dict, registry_path: Path) -> None:
    source = registry_path if registry_path.exists() else BOOTSTRAP_BOARDS_PATH
    boards = json.loads(source.read_text())
    old_key = f'{old_board["provider"]}:{old_board["slug"]}'
    new_key = f'{new_board["provider"]}:{new_board["slug"]}'
    updated = []
    replaced = False
    for board in boards:
        key = f'{board.get("provider")}:{board.get("slug")}'
        if key == new_key:
            continue
        if key == old_key:
            updated.append({**board, **new_board})
            replaced = True
        else:
            updated.append(board)
    if not replaced:
        updated.append(new_board)
    temporary = registry_path.with_name(f".{registry_path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(updated, indent=2) + "\n")
    os.replace(temporary, registry_path)


def apply_board_migration(
    connection: sqlite3.Connection,
    old_board: dict,
    new_board: dict,
    registry_path: Path,
) -> None:
    old_key = f'{old_board["provider"]}:{old_board["slug"]}'
    new_key = f'{new_board["provider"]}:{new_board["slug"]}'
    # The company was already baselined under its old board; don't swallow its
    # fresh jobs as a second silent baseline.
    if connection.execute(
        "SELECT 1 FROM initialized_boards WHERE board_key = ?", (old_key,)
    ).fetchone():
        connection.execute(
            "INSERT OR IGNORE INTO initialized_boards(board_key) VALUES (?)", (new_key,)
        )
    connection.execute("DELETE FROM board_stats WHERE board_key = ?", (old_key,))
    save_board_migration(old_board, new_board, registry_path)


def deliver_email(subject: str, text_body: str, html_body: str, idempotency_key: str) -> None:
    recipient = os.environ.get("JOB_WATCHER_TO")
    if not recipient:
        raise RuntimeError("JOB_WATCHER_TO is not configured")
    resend_key = os.environ.get("JOB_WATCHER_RESEND_API_KEY")
    if resend_key:
        payload = json.dumps({
            "from": "Job Watcher <onboarding@resend.dev>",
            "to": [recipient],
            "subject": subject,
            "text": text_body,
            "html": html_body,
        }).encode()
        request = urllib.request.Request(
            "https://api.resend.com/emails",
            data=payload,
            headers={
                "Authorization": f"Bearer {resend_key}",
                "Content-Type": "application/json",
                "Idempotency-Key": idempotency_key,
                "User-Agent": "personal-new-grad-job-watcher/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30):
            return

    sender = os.environ.get("JOB_WATCHER_GMAIL", recipient)
    password = os.environ.get("JOB_WATCHER_GMAIL_APP_PASSWORD")
    if not password:
        raise RuntimeError("No email provider is configured")
    message = EmailMessage()
    message["From"], message["To"], message["Subject"] = sender, recipient, subject
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as smtp:
        smtp.login(sender, password.replace(" ", ""))
        smtp.send_message(message)



PUSH_MAX_PER_RUN = 5


def send_push(jobs: list[dict]) -> None:
    """Phone alerts through ntfy, sent before the email so you can apply first."""
    topic = os.environ.get("JOB_WATCHER_NTFY_TOPIC")
    if not topic or not jobs:
        return
    server = os.environ.get("JOB_WATCHER_NTFY_SERVER", "https://ntfy.sh").rstrip("/")
    ordered = sorted(jobs, key=lambda job: -(job_posted_timestamp(job) or 0))
    messages = [{
        "topic": topic,
        "title": f'{display_company_name(job.get("company_name"))}: {job.get("title", "")}'[:250],
        "message": " · ".join(filter(None, [
            ", ".join(str(value) for value in (job.get("locations") or [])[:2]),
            f"Posted {posted_label(job)}",
            experience_summary(job),
        ])),
        "click": str(job.get("url") or ""),
        "tags": ["briefcase"],
        "priority": 4,
    } for job in ordered[:PUSH_MAX_PER_RUN]]
    if len(jobs) > PUSH_MAX_PER_RUN:
        messages.append({
            "topic": topic,
            "title": f"+{len(jobs) - PUSH_MAX_PER_RUN} more new roles",
            "message": "The email has the full list.",
            "priority": 3,
        })
    for message in messages:
        request = urllib.request.Request(
            server, data=json.dumps(message).encode(), method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "personal-new-grad-job-watcher/1.0"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10):
                pass
        except Exception as exc:
            print(f"Push notification failed: {exc}", file=sys.stderr)


def display_company_name(value: object) -> str:
    raw = plain_text(str(value or "Unknown company")).strip()
    if not raw:
        return "Unknown company"

    special_names = {
        "sonyinteractiveentertainmentglobal": "Sony Interactive Entertainment Global",
        "sony interactive entertainment global": "Sony Interactive Entertainment Global",
    }
    normalized = raw.lower().replace("-", " ").replace("_", " ").strip()
    if normalized in special_names:
        return special_names[normalized]

    cleaned = raw.replace("_", " ").replace("-", " ").strip()
    if " " in cleaned:
        return " ".join(part.capitalize() if part.islower() else part for part in cleaned.split())

    return cleaned


def send_email(jobs: list[dict], subject_prefix: str = "") -> None:
    subject = f"{subject_prefix}{len(jobs)} new US tech job{'s' if len(jobs) != 1 else ''}"
    rows = []
    text_rows = [
        f"{'Cat':<8} {'Role':<42} {'Company':<38} {'Experience':<14} Posted",
        f"{'-' * 8} {'-' * 42} {'-' * 38} {'-' * 14} {'-' * 10}",
    ]
    category_order = {name: index for index, name in enumerate(
        ("ml", "ai", "data_science", "data", "sde", "robotics", "other")
    )}
    jobs = sorted(
        jobs,
        key=lambda job: (
            category_order.get(role_category(job), 99),
            -(job_posted_timestamp(job) or 0),
            str(job.get("company_name", "")).lower(),
        ),
    )
    previous_category = None
    for job in jobs:
        category = html.escape(role_category(job))
        role = html.escape(str(job.get("title", "Untitled role")))
        company = html.escape(display_company_name(job.get("company_name", "Unknown company")))
        experience = html.escape(experience_summary(job))
        posted = html.escape(posted_label(job))
        url = html.escape(str(job.get("url", "")), quote=True)
        text_category = plain_text(role_category(job))[:8]
        text_role = plain_text(str(job.get("title", "Untitled role")))[:42]
        text_company = display_company_name(job.get("company_name", "Unknown company"))[:38]
        text_experience = plain_text(experience_summary(job))[:14]
        text_posted = plain_text(posted_label(job))
        text_rows.append(
            f"{text_category:<8} {text_role:<42} {text_company:<38} {text_experience:<14} {text_posted}"
        )
        previous_category = category
        rows.append(
            "<tr>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\"><strong><a href=\"{}\">{}</a></strong></td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "</tr>".format(
                category,
                url,
                role,
                company,
                experience,
                posted,
            )
        )
    html_body = (
        "<div style=\"font-family:Arial,sans-serif;line-height:1.4;max-width:720px;margin:0 auto;\">"
        "<h2 style=\"margin:0 0 12px 0;\">New matching jobs</h2>"
        "<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%;border-collapse:collapse;table-layout:fixed;\">"
        "<thead><tr>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:12%;\">Cat</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:34%;\">Role</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:22%;\">Company</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:18%;\">Experience</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:14%;\">Posted</th>"
        "</tr></thead>"
        "<tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )
    text_body = "\n".join(text_rows)
    idempotency_seed = "|".join(sorted(stable_id(job) for job in jobs))
    if subject.startswith("Digest:"):
        idempotency_seed = f"{idempotency_seed}|{int(time.time())}"
    idempotency_key = hashlib.sha256(idempotency_seed.encode()).hexdigest()
    deliver_email(subject, text_body, html_body, idempotency_key)


def record_metrics(
    connection: sqlite3.Connection,
    run_at: int,
    source: str,
    jobs: list[dict],
    new_jobs: int = 0,
    failures: int = 0,
) -> None:
    rejections = Counter(rejection_reason(job, run_at) for job in jobs)
    connection.execute(
        "INSERT INTO run_metrics VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            run_at,
            source,
            len(jobs),
            rejections.get("matched", 0),
            new_jobs,
            failures,
            json.dumps(rejections, sort_keys=True),
        ),
    )


def health_report(connection: sqlite3.Connection, now: int) -> tuple[str, str, str]:
    since = now - HEALTH_REPORT_INTERVAL_SECONDS
    rows = connection.execute(
        "SELECT source, fetched, matched, new_jobs, failures, rejections "
        "FROM run_metrics WHERE run_at >= ?",
        (since,),
    ).fetchall()
    totals = Counter()
    rejections = Counter()
    failures_by_source = Counter()
    sources = set()
    for source, fetched, matched_count, new_count, failures, reasons in rows:
        if source != "notifications":
            sources.add(source)
        totals.update({
            "fetched": fetched,
            "matched": matched_count,
            "new": new_count,
            "failures": failures,
        })
        if failures:
            failures_by_source[source] += failures
        rejections.update(json.loads(reasons))
    subject = f"Job Watcher daily health: {totals['new']} alerts"
    summary = [
        "Job Watcher ran during the last 24 hours.",
        f"Sources checked: {len(sources)}",
        f"Jobs inspected: {totals['fetched']}",
        f"Jobs matching filters: {totals['matched']}",
        f"New jobs emailed: {totals['new']}",
        f"Source failures: {totals['failures']}",
        "",
        "Failures by source:",
    ]
    summary.extend(
        f"- {source}: {count}"
        for source, count in failures_by_source.most_common()
    )
    if not failures_by_source:
        summary.append("- none")
    summary.extend(["", "Filter outcomes:"])
    summary.extend(f"- {key}: {value}" for key, value in rejections.most_common())
    text_body = "\n".join(summary)
    html_body = "<div style=\"font-family:Arial,sans-serif\"><h2>Job Watcher health</h2>" + "".join(
        f"<p style=\"margin:4px 0\">{html.escape(line)}</p>" for line in summary
    ) + "</div>"
    return subject, text_body, html_body


def send_health_report_if_due(
    connection: sqlite3.Connection, now: int, force: bool = False
) -> bool:
    row = connection.execute(
        "SELECT last_checked FROM source_checks WHERE source = 'health_report'"
    ).fetchone()
    if not force and row and now - int(row[0]) < HEALTH_REPORT_INTERVAL_SECONDS:
        return False
    subject, text_body, html_body = health_report(connection, now)
    deliver_email(subject, text_body, html_body, f"health-{now // HEALTH_REPORT_INTERVAL_SECONDS}")
    connection.execute(
        "INSERT INTO source_checks(source, last_checked) VALUES ('health_report', ?) "
        "ON CONFLICT(source) DO UPDATE SET last_checked=excluded.last_checked",
        (now,),
    )
    return True


def source_candidates(
    connection: sqlite3.Connection, source_key: str, jobs: list[dict]
) -> tuple[list[dict], bool]:
    if source_key.startswith("workable:"):
        observed = {
            row[0] for row in connection.execute(
                "SELECT job_id FROM observed_source_jobs WHERE source = ?",
                (source_key,),
            )
        }
        job_ids = {stable_id(job) for job in jobs}
        if not observed:
            connection.executemany(
                "INSERT OR IGNORE INTO observed_source_jobs(source, job_id) VALUES (?, ?)",
                [(source_key, job_id) for job_id in job_ids],
            )
            connection.execute(
                "INSERT OR IGNORE INTO initialized_boards(board_key) VALUES (?)",
                (source_key,),
            )
            return [], True
        detected_at = int(time.time())
        for job in jobs:
            if stable_id(job) not in observed:
                job["detected_at"] = detected_at
                if job.get("posted_at") and DATE_ONLY_RE.match(str(job["posted_at"]).strip()):
                    job["posted_precision"] = "date"
        connection.executemany(
            "INSERT OR IGNORE INTO observed_source_jobs(source, job_id) VALUES (?, ?)",
            [(source_key, job_id) for job_id in job_ids],
        )
    matching = [job for job in jobs if matches(job)]
    initialized = connection.execute(
        "SELECT 1 FROM initialized_boards WHERE board_key = ?", (source_key,)
    ).fetchone()
    if not initialized:
        record_jobs(connection, matching)
        connection.execute(
            "INSERT OR IGNORE INTO initialized_boards(board_key) VALUES (?)", (source_key,)
        )
        return [], True
    return [job for job in matching if not job_seen(connection, job)], False

DIGEST_TITLE_TERMS = [
    "software engineer",
    "software developer",
    "backend engineer",
    "frontend engineer",
    "full stack engineer",
    "full-stack engineer",
    "data engineer",
    "machine learning engineer",
    "ml engineer",
    "ai engineer",
    "platform engineer",
    "cloud engineer",
    "infrastructure engineer",
    "systems engineer",
    "site reliability engineer",
    "sre",
    "devops engineer",
    "security engineer",
    "application engineer",
    "new grad",
    "university graduate",
    "early career",
    "entry level",
    "associate engineer",
    "software engineer i",
    "software developer i",
]

DIGEST_BLOCKED_TITLE_TERMS = [
    "senior",
    "sr.",
    "sr ",
    "staff",
    "principal",
    "lead",
    "manager",
    "director",
    "architect",
    "intern",
    "internship",
    "co-op",
    "coop",
    "student",
    "phd",
    "postdoc",
    "founding engineer",
    "head of",
    "vp ",
    "recruiter",
    "customer experience",
    "customer support",
    "business development",
    "sales",
    "finance",
    "coordinator",
    "associate, finance",
    "finance associate",
    "analyst",
    "production support",
    "independent consultant",
    "w2 position",
]



US_STATE_TERMS = [
    "united states", "usa", "u.s.", "us remote", "remote us", "remote, us",
    "new york", "ny", "california", "ca", "san francisco", "sf", "bay area",
    "seattle", "wa", "washington", "boston", "ma", "massachusetts",
    "austin", "tx", "texas", "dallas", "houston", "chicago", "il",
    "illinois", "atlanta", "ga", "georgia", "denver", "co", "colorado",
    "los angeles", "la", "san diego", "palo alto", "sunnyvale",
    "mountain view", "santa clara", "san jose", "cupertino",
    "miami", "fl", "florida", "phoenix", "az", "arizona",
    "portland", "or", "oregon", "raleigh", "nc", "north carolina",
    "charlotte", "nashville", "tn", "tennessee", "salt lake city", "utah",
    "arlington", "virginia", "va", "washington dc", "dc",
]

NON_US_LOCATION_TERMS = [
    "canada", "toronto", "vancouver", "montreal", "ontario",
    "united kingdom", "uk", "london", "england", "scotland", "ireland",
    "europe", "emea", "eu ", "european union", "germany", "berlin", "munich",
    "france", "paris", "spain", "madrid", "barcelona", "netherlands",
    "amsterdam", "poland", "warsaw", "sweden", "stockholm",
    "india", "bangalore", "bengaluru", "hyderabad", "pune", "mumbai",
    "delhi", "gurgaon", "noida", "chennai",
    "korea", "south korea", "seoul", "대한민국", "채용관심등록",
    "japan", "tokyo", "singapore", "australia", "sydney", "melbourne",
    "new zealand", "mexico", "brazil", "argentina", "latin america",
    "latam", "israel", "tel aviv", "dubai", "uae",
]


def digest_is_us_location(job: dict) -> bool:
    locations = job.get("locations") or []
    location_text = " ".join(str(value) for value in locations).lower()
    title = str(job.get("title", "")).lower()
    description = str(job.get("description", "")).lower()
    combined = f"{title} {location_text} {description}"

    if any(term in combined for term in NON_US_LOCATION_TERMS):
        return False

    if any(term in location_text for term in US_STATE_TERMS):
        return True

    # Allow plain remote only when no non-US signal exists.
    if location_text.strip() in {"remote", "remote - us", "remote us", "remote, us"}:
        return True

    return False



SPONSORSHIP_RISK_COMPANIES = {
    "andurilindustries",
    "anduril industries",
    "spacex",
    "wyetechllc",
    "wyetech",
    "syncreon consulting",
}

SPONSORSHIP_RISK_TITLE_TERMS = [
    "public sector",
    "federal",
    "defense",
    "dod",
    "clearance",
    "security clearance",
    "itar",
    "export control",
    "anti-tamper",
    "classified",
    "intelligence systems",
    "sentry tower",
    "surveillance tower",
]


def has_sponsorship_risk(job: dict) -> bool:
    title = plain_text(str(job.get("title", ""))).lower()
    company = plain_text(str(job.get("company_name", ""))).lower()
    description = plain_text(str(job.get("description", ""))).lower()
    combined = f"{title} {company} {description}"

    if company in SPONSORSHIP_RISK_COMPANIES:
        return True

    if any(term in combined for term in SPONSORSHIP_RISK_TITLE_TERMS):
        return True

    return False



def digest_has_real_recent_posted_time(job: dict, now: int | None = None) -> bool:
    current_time = int(time.time()) if now is None else now

    # Use real posting/publishing fields only.
    # Do not use updated_at here, because old jobs can be edited/reindexed months later.
    timestamp_fields = [
        "posted_at",
        "published_at",
        "publishedAt",
        "created_at",
        "createdAt",
    ]

    for field in timestamp_fields:
        value = job.get(field)
        if value and source_timestamp_is_recent(value, current_time):
            return True

    return False


def digest_match(job: dict) -> bool:
    title = str(job.get("title", "")).lower()
    description = str(job.get("description", ""))
    locations = " ".join(str(value) for value in job.get("locations", []))

    if not job.get("active", True) or job.get("is_visible") is False:
        return False
    if not job_is_recent(job):
        return False
    if not digest_is_us_location(job):
        return False
    if rejection_reason(job) in {"outside_us", "citizenship_requirement"}:
        return False
    if has_sponsorship_risk(job):
        return False
    if asks_for_too_much_experience(f"{title} {description}"):
        return False
    if any(term in title for term in DIGEST_BLOCKED_TITLE_TERMS):
        return False
    if "korea" in locations.lower() or "대한민국" in locations.lower():
        return False

    engineering_terms = [
        "software engineer",
        "software developer",
        "backend engineer",
        "frontend engineer",
        "full stack engineer",
        "full-stack engineer",
        "data engineer",
        "machine learning engineer",
        "ml engineer",
        "ai engineer",
        "platform engineer",
        "cloud engineer",
        "infrastructure engineer",
        "systems engineer",
        "site reliability engineer",
        "sre",
        "devops engineer",
        "security engineer",
        "application engineer",
        "robotics software engineer",
        "flight software",
        "embedded",
    ]

    return any(term in title for term in engineering_terms)


def select_digest_boards(connection: sqlite3.Connection, boards: list[dict]) -> list[dict]:
    failing = {
        row[0]
        for row in connection.execute(
            "SELECT board_key FROM board_stats WHERE failure_count >= 3"
        )
    }
    selected = []
    for board in boards:
        key = f'{board.get("provider")}:{board.get("slug")}'
        if key not in failing:
            selected.append(board)
    return selected



EXA_SEARCH_QUERIES = [
    'new grad software engineer United States apply careers',
    'software engineer new graduate United States careers',
    'software engineer university graduate United States careers',
    'software engineer early career United States careers',
    'software engineer I United States careers',
    'software engineer 1 United States careers',
    'junior software engineer United States careers',
    'entry level software engineer United States careers',
    'backend engineer new grad United States careers',
    'backend engineer entry level United States careers',
    'platform engineer new grad United States careers',
    'infrastructure engineer new grad United States careers',
    'machine learning engineer new grad United States careers',
    'AI engineer new grad United States careers',
    'data engineer new grad United States careers',
    'systems engineer new grad United States careers',
]

EXA_INCLUDED_DOMAINS = [
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "workdayjobs.com",
    "myworkdayjobs.com",
    "smartrecruiters.com",
    "jobs.lever.co",
    "job-boards.greenhouse.io",
]


def company_from_url(url: str) -> str:
    lowered = url.lower()
    for marker in [
        "job-boards.greenhouse.io/",
        "boards.greenhouse.io/",
        "jobs.lever.co/",
        "jobs.ashbyhq.com/",
        "smartrecruiters.com/",
    ]:
        if marker in lowered:
            tail = lowered.split(marker, 1)[1]
            company = tail.split("/", 1)[0]
            return display_company_name(company)
    return "Unknown company"


def fetch_exa_jobs(now: int) -> list[dict]:
    if os.environ.get("EXA_ENABLED") != "1":
        return []

    api_key = os.environ.get("EXA_API_KEY")
    if not api_key:
        return []

    start_date = datetime.fromtimestamp(
        now - MAX_JOB_AGE_SECONDS,
        tz=timezone.utc,
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    normalized = []
    seen_urls = set()
    raw_count = 0

    for query in EXA_SEARCH_QUERIES:
        payload = json.dumps({
            "query": query,
            "type": "auto",
            "numResults": 30,
            "userLocation": "US",
            "contents": {
                "text": {
                    "maxCharacters": 3000
                },
                "highlights": True
            }
        }).encode()

        request = urllib.request.Request(
            "https://api.exa.ai/search",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "User-Agent": "personal-new-grad-job-watcher/1.0",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode())
        except Exception as exc:
            print(f"Exa source failed query={query!r}: {exc}", file=sys.stderr)
            continue

        results = data.get("results", [])
        raw_count += len(results)

        for item in results:
            url = str(item.get("url", "")).strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            title = plain_text(str(item.get("title", "") or "Untitled role"))
            text_content = plain_text(str(item.get("text", "") or ""))
            highlights = " ".join(
                plain_text(str(value))
                for value in item.get("highlights", []) or []
            )
            description = " ".join(part for part in [title, highlights, text_content] if part)

            published = item.get("publishedDate") or item.get("published_date")

            # Be strict for Exa: if Exa does not provide a real published date,
            # do not pretend the job is new. This prevents 3-6 month old roles
            # from entering the 24-hour digest.
            if not published:
                continue

            if not source_timestamp_is_recent(published, now):
                continue

            normalized.append({
                "id": f"exa:{hashlib.sha256(url.encode()).hexdigest()[:16]}",
                "source": "exa",
                "title": title,
                "company_name": company_from_url(url),
                "locations": ["United States"],
                "url": url,
                "description": description,
                "posted_at": published,
                "active": True,
                "is_visible": True,
            })

    return normalized


def send_digest(connection: sqlite3.Connection, boards: list[dict], feeds: list[dict]) -> None:
    checked_at = int(time.time())
    digest_boards = select_digest_boards(connection, boards)

    direct_results, failed_boards = fetch_direct_batch(digest_boards)
    aggregator_results = fetch_due_aggregators(connection, checked_at)
    feed_results, failed_feeds = fetch_due_feeds(connection, feeds, checked_at)
    exa_results = fetch_exa_jobs(checked_at)

    all_jobs = []
    for _, jobs in direct_results:
        all_jobs.extend(jobs)
    for _, jobs in aggregator_results:
        all_jobs.extend(jobs)
    for _, jobs in feed_results:
        all_jobs.extend(jobs)
    all_jobs.extend(exa_results)

    strict_rejections = Counter(rejection_reason(job, checked_at) for job in all_jobs)
    digest_matches = [job for job in all_jobs if digest_match(job)]

    print("Strict filter outcomes:")
    for reason, count in strict_rejections.most_common():
        print(f"- {reason}: {count}")
    print(f"Broader digest matches: {len(digest_matches)}")

    candidates = digest_matches

    unique_jobs = []
    seen_keys = set()
    for job in sorted(
        candidates,
        key=lambda item: job_posted_timestamp(item) or 0,
        reverse=True,
    ):
        title_key = plain_text(str(job.get("title", ""))).lower().strip()
        company_key = plain_text(str(job.get("company_name", ""))).lower().strip()

        # Deduplicate primarily by title + company.
        # Location-level dedupe caused repeated multi-city postings like Speechify/Anduril.
        key = f"{title_key}|{company_key}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_jobs.append(job)

    digest_jobs = unique_jobs
    if not digest_jobs:
        print(
            f"Digest found 0 matching jobs; checked {len(direct_results)} ATS boards, "
            f"failed {len(failed_boards)}, aggregators {len(aggregator_results)}, "
            f"feeds {len(feed_results)}, exa {len(exa_results)}."
        )
        return

    send_email(digest_jobs, subject_prefix="Digest: ")
    print(
        f"Digest emailed {len(digest_jobs)} matching jobs; checked {len(direct_results)} ATS boards, "
        f"failed {len(failed_boards)}, aggregators {len(aggregator_results)}, "
        f"feeds {len(feed_results)}, exa {len(exa_results)}."
    )


def main() -> int:
    if "--test-push" in sys.argv:
        send_push([{
            "id": f"push-test-{time.time_ns()}",
            "title": "Push test: you will get alerts like this",
            "company_name": "Job Watcher",
            "locations": ["United States"],
            "url": "https://github.com/Abhayrkhot/job-watcher",
            "posted_at": int(time.time()),
        }])
        print("Push notification sent" if os.environ.get("JOB_WATCHER_NTFY_TOPIC")
              else "JOB_WATCHER_NTFY_TOPIC is not set")
        return 0
    if "--test-email" in sys.argv:
        send_email([{
            "id": f"delivery-test-{time.time_ns()}",
            "title": "Job Watcher Delivery Test",
            "company_name": "Job Watcher",
            "locations": ["United States / Remote"],
            "url": "https://developers.greenhouse.io/job-board",
        }])
        print("Test email accepted by the email provider")
        return 0
    boards = load_boards()
    feeds = load_feeds()
    if not boards:
        raise RuntimeError("No approved ATS boards are configured")
    with connect_db() as connection:
        checked_at = int(time.time())
        if "--health-report" in sys.argv:
            send_health_report_if_due(connection, checked_at, force=True)
            print("Health report accepted by the email provider")
            return 0
        if "--digest" in sys.argv:
            send_digest(connection, boards, feeds)
            return 0
        heartbeat, last_run = heartbeat_message(connection, checked_at)
        print(heartbeat)
        if last_run is not None and checked_at - last_run > HEARTBEAT_GRACE_SECONDS:
            print(
                f"job-watcher: missed at least one 5-minute run; gap={checked_at - last_run}s",
                file=sys.stderr,
            )
        if not wait_for_network():
            print(
                "job-watcher: network unavailable; skipped this run without "
                "touching board state",
                file=sys.stderr,
            )
            return 0
        due_boards = prepare_boards(
            connection, select_due_boards(connection, boards, checked_at)
        )
        direct_results, failed_boards = fetch_direct_batch(due_boards)
        missing_boards = [
            board for board, exc in failed_boards if is_missing_board_error(exc)
        ]
        unreachable_boards = [
            board for board, exc in failed_boards if is_network_error(exc)
        ]
        transient_boards = [
            board for board, exc in failed_boards if is_transient_http_error(exc)
        ]
        broken_boards = [
            board for board, exc in failed_boards
            if not is_missing_board_error(exc) and not is_network_error(exc)
            and not is_transient_http_error(exc)
        ]
        outage = network_outage(len(direct_results), len(unreachable_boards))
        if outage:
            # Aggregators and feeds would fail too and burn their polling slot.
            print(
                f"job-watcher: {len(unreachable_boards)} of {len(due_boards)} boards "
                "unreachable; treating it as a network outage and keeping them due",
                file=sys.stderr,
            )
            aggregator_results, feed_results, failed_feeds = [], [], []
        else:
            aggregator_results = fetch_due_aggregators(connection, checked_at)
            aggregator_results += fetch_simplify(connection, boards, checked_at, BOARDS_PATH)
            feed_results, failed_feeds = fetch_due_feeds(connection, feeds, checked_at)
        candidates = []
        initialized_count = 0
        metric_jobs: dict[str, list[dict]] = {}
        metric_failures = Counter(
            f'ats:{board["provider"]}' for board, _ in failed_boards
        )
        not_modified = 0
        for board, direct_jobs in direct_results:
            if board.get("_not_modified"):
                record_board_checked(connection, board, checked_at)
                not_modified += 1
                continue
            board_key = f'{board["provider"]}:{board["slug"]}'
            metric_jobs.setdefault(f'ats:{board["provider"]}', []).extend(direct_jobs)
            relevant_count = sum(1 for job in direct_jobs if potential_match(job))
            match_count = sum(1 for job in direct_jobs if matches(job))
            record_board_success(
                connection, board, relevant_count, match_count, checked_at
            )
            remember_listed_ids(connection, board)
            remember_etag(connection, board)
            new_candidates, was_initialized = source_candidates(
                connection, board_key, direct_jobs
            )
            candidates.extend(new_candidates)
            initialized_count += int(was_initialized)
        for board in broken_boards:
            record_board_failure(connection, board, checked_at)
        for board in transient_boards:
            record_board_unreachable(connection, board, checked_at)
        if not outage:
            for board in unreachable_boards:
                record_board_unreachable(connection, board, checked_at)
        moved_count = 0
        for old_board, new_board in find_board_migrations(missing_boards):
            if new_board is None:
                record_board_failure(connection, old_board, checked_at)
                continue
            apply_board_migration(connection, old_board, new_board, BOARDS_PATH)
            moved_count += 1
            print(
                f'Board moved: {new_board["migrated_from"]} -> '
                f'{new_board["provider"]}:{new_board["slug"]}'
            )
        for source, jobs in aggregator_results:
            metric_jobs.setdefault(f"aggregator:{source}", []).extend(jobs)
            new_candidates, was_initialized = source_candidates(
                connection, f"aggregator:{source}", jobs
            )
            candidates.extend(new_candidates)
            initialized_count += int(was_initialized)
        for source, jobs in feed_results:
            metric_jobs.setdefault(source, []).extend(jobs)
            new_candidates, was_initialized = source_candidates(connection, source, jobs)
            candidates.extend(new_candidates)
            initialized_count += int(was_initialized)
        metric_failures.update(failed_feeds)

        new_jobs = []
        candidate_keys = set()
        for job in candidates:
            key = canonical_url(job) or stable_id(job)
            if key not in candidate_keys:
                candidate_keys.add(key)
                new_jobs.append(job)
        if new_jobs and "--initialize" not in sys.argv:
            send_push(new_jobs)
            send_email(new_jobs)
        record_jobs(connection, new_jobs)
        for source, jobs in metric_jobs.items():
            record_metrics(
                connection,
                checked_at,
                source,
                jobs,
                failures=metric_failures.pop(source, 0),
            )
        for source, failure_count in metric_failures.items():
            record_metrics(connection, checked_at, source, [], failures=failure_count)
        record_metrics(
            connection,
            checked_at,
            "notifications",
            [],
            new_jobs=len(new_jobs) if "--initialize" not in sys.argv else 0,
        )
        connection.execute(
            "DELETE FROM run_metrics WHERE run_at < ?",
            (checked_at - 7 * HEALTH_REPORT_INTERVAL_SECONDS,),
        )
        record_heartbeat(connection, checked_at)
        if "--initialize" not in sys.argv:
            try:
                send_health_report_if_due(connection, checked_at)
            except Exception as exc:
                print(f"Health report failed: {exc}", file=sys.stderr)
    print(
        f"Selected {len(due_boards)} ATS boards; checked {len(direct_results)} "
        f"({not_modified} unchanged); "
        f"failed {len(failed_boards)} (unreachable {len(unreachable_boards)}"
        f"{', outage' if outage else ''}; moved {moved_count}); "
        f"aggregators {len(aggregator_results)}; "
        f"feeds {len(feed_results)} (failed {len(failed_feeds)}); "
        f"initialized {initialized_count}; recorded {len(new_jobs)} new matching jobs"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"job-watcher: {exc}", file=sys.stderr)
        raise SystemExit(1)
