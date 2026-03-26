import os, time, json, hashlib, re, logging, asyncio
from datetime import datetime
import httpx
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("deal-radar")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://idxmuhnajmrzoxnsqzpo.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "User-Agent": "DealRadar-Bot/1.0 (+https://joindealradar.com/bot)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ─── MARKET COMPS ────────────────────────────────────────
COMPS = {
    "Excavators":         (95000,  4500),
    "Skid Steers":        (52000,  2800),
    "Dozers":             (140000, 6000),
    "Trucks & Trailers":  (85000,  3000),
    "Wheel Loaders":      (110000, 5000),
    "Graders":            (120000, 5500),
    "Cranes":             (200000, 8000),
    "Compactors":         (65000,  3000),
    "Other Equipment":    (50000,  2000),
}

KEYWORDS = {
    "Excavators":        ["excavat","trackhoe","hoe","backhoe"],
    "Skid Steers":       ["skid steer","skidsteer","bobcat","track loader"],
    "Dozers":            ["dozer","bulldozer"],
    "Trucks & Trailers": ["truck","trailer","semi","pickup","dump"],
    "Wheel Loaders":     ["wheel loader","front loader","payloader"],
    "Graders":           ["grader","motor grader"],
    "Cranes":            ["crane","boom lift"],
    "Compactors":        ["compactor","roller","packer"],
}

def categorize(title):
    t = title.lower()
    for cat, kws in KEYWORDS.items():
        if any(k in t for k in kws):
            return cat
    return "Other Equipment"

def estimate_value(title, hours, year, condition):
    cat = categorize(title)
    base, hdep = COMPS.get(cat, (50000, 2000))
    age = datetime.now().year - (year or 2015)
    val = base - (age * 0.08 * base) - ((hours or 2000) / 1000 * hdep)
    val = max(val, base * 0.15)
    mults = {"Excellent": 1.10, "Good": 1.0, "Fair": 0.82, "Poor": 0.60}
    return round(val * mults.get(condition, 1.0))

def score(bid, value, hours, year, condition, end_date):
    if not value or not bid or bid <= 0:
        return 50
    disc = (value - bid) / value
    s = 50
    if disc >= 0.40:   s += 40
    elif disc >= 0.30: s += 32
    elif disc >= 0.20: s += 22
    elif disc >= 0.10: s += 12
    elif disc >= 0.0:  s += 4
    else:              s -= 10
    cond = {"Excellent": 10, "Good": 6, "Fair": 2, "Poor": -5}
    s += cond.get(condition, 0)
    if hours:
        if hours < 500:    s += 8
        elif hours < 1500: s += 5
        elif hours < 3000: s += 2
        elif hours > 8000: s -= 5
    if year:
        age = datetime.now().year - year
        if age <= 2:   s += 7
        elif age <= 5: s += 4
        elif age > 15: s -= 3
    try:
        end = datetime.fromisoformat(end_date.replace("Z",""))
        days = (end - datetime.now()).days
        if days <= 1:   s += 5
        elif days <= 3: s += 3
    except:
        pass
    return max(0, min(100, s))

def parse_hours(text):
    if not text: return None
    m = re.search(r"[\d,]+", text.replace(",",""))
    return int(m.group().replace(",","")) if m else None

def parse_price(text):
    if not text: return 0
    return float(re.sub(r"[^\d.]","", text) or 0)

def parse_year(text):
    m = re.search(r"(19|20)\d{2}", str(text))
    return int(m.group()) if m else None

def make_id(source, lot_id):
    return hashlib.sha256(f"{source}:{lot_id}".encode()).hexdigest()[:16]

# ─── GOVPLANET SCRAPER ───────────────────────────────────
async def scrape_govplanet(client):
    listings = []
    categories = [
        ("excavator",    "https://www.govplanet.com/for-sale/Excavators/ci/10260?lang=en_US&usedOnly=1&sortby=ad"),
        ("skid steer",   "https://www.govplanet.com/for-sale/Skid-Steer-Loaders/ci/10276?lang=en_US&usedOnly=1&sortby=ad"),
        ("dozer",        "https://www.govplanet.com/for-sale/Crawler-Dozers/ci/10249?lang=en_US&usedOnly=1&sortby=ad"),
        ("truck",        "https://www.govplanet.com/for-sale/Trucks-Truck-Tractors/ci/10286?lang=en_US&usedOnly=1&sortby=ad"),
        ("wheel loader", "https://www.govplanet.com/for-sale/Wheel-Loaders/ci/10291?lang=en_US&usedOnly=1&sortby=ad"),
        ("grader",       "https://www.govplanet.com/for-sale/Motor-Graders/ci/10268?lang=en_US&usedOnly=1&sortby=ad"),
    ]
    for cat_name, url in categories:
        try:
            log.info(f"Scraping GovPlanet: {cat_name}")
            resp = await client.get(url, timeout=20)
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select(".item-card-container, .search-result-item, [data-item-id]")
            log.info(f"  Found {len(cards)} cards")
            for card in cards[:15]:
                try:
                    # Title
                    title_el = card.select_one("h2 a, h3 a, .item-title a, .result-title a")
                    if not title_el: continue
                    title = title_el.get_text(strip=True)
                    link  = "https://www.govplanet.com" + title_el.get("href","") if title_el.get("href","").startswith("/") else title_el.get("href","")

                    # Price
                    price_el = card.select_one(".price, .buy-now-price, .current-bid, [class*='price']")
                    price = parse_price(price_el.get_text() if price_el else "0")

                    # Image
                    img_el = card.select_one("img[src*='govplanet'], img[src*='ironplanet'], img[src*='cdn'], img[src*='http']")
                    img = img_el.get("src","") or img_el.get("data-src","") if img_el else ""

                    # Location
                    loc_el = card.select_one(".location, [class*='location'], [class*='city']")
                    location = loc_el.get_text(strip=True) if loc_el else "United States"

                    # Hours/year from title
                    hours = parse_hours(card.get_text())
                    year  = parse_year(title)

                    # Lot ID
                    lot_id = card.get("data-item-id") or card.get("id") or link.split("/")[-1] or title[:20]

                    condition = "Good"
                    end_date  = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    category  = categorize(title)
                    value     = estimate_value(title, hours, year, condition)
                    deal_score = score(price, value, hours, year, condition, end_date)

                    listings.append({
                        "id":              make_id("GovPlanet", lot_id),
                        "source_id":       str(lot_id),
                        "source_name":     "GovPlanet",
                        "source_url":      link,
                        "title":           title,
                        "current_bid":     price,
                        "location":        location,
                        "state":           location.split(",")[-1].strip()[:2] if "," in location else "",
                        "description":     title,
                        "image_url":       img,
                        "auction_end":     end_date,
                        "category":        category,
                        "asset_type":      "equipment",
                        "condition":       condition,
                        "hours":           hours,
                        "year":            year,
                        "score":           deal_score,
                        "estimated_value": value,
                        "scraped_at":      datetime.utcnow().isoformat(),
                    })
                except Exception as e:
                    log.debug(f"Card error: {e}")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"Category error {cat_name}: {e}")
    return listings

# ─── PURPLE WAVE SCRAPER ─────────────────────────────────
async def scrape_purplewave(client):
    listings = []
    urls = [
        "https://www.purplewave.com/auction/search?q=excavator&sort=end_asc&format=json",
        "https://www.purplewave.com/auction/search?q=dozer&sort=end_asc&format=json",
        "https://www.purplewave.com/auction/search?q=skid+steer&sort=end_asc&format=json",
        "https://www.purplewave.com/auction/search?q=truck&sort=end_asc&format=json",
    ]
    for url in urls:
        try:
            resp = await client.get(url, timeout=20)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    items = data.get("items", data.get("results", data.get("lots", [])))
                    for item in items[:10]:
                        title    = item.get("title","") or item.get("name","")
                        price    = float(item.get("current_bid", item.get("price", 0)) or 0)
                        img      = item.get("image_url","") or item.get("photo","") or item.get("thumbnail","")
                        location = item.get("location","") or item.get("city","")
                        lot_id   = str(item.get("id","") or item.get("lot_id",""))
                        link     = item.get("url","") or f"https://www.purplewave.com/lot/{lot_id}"
                        hours    = parse_hours(str(item.get("hours","")))
                        year     = parse_year(str(item.get("year","")) or title)
                        end_date = item.get("end_date","") or datetime.now().isoformat()
                        condition= item.get("condition","Good") or "Good"
                        category = categorize(title)
                        value    = estimate_value(title, hours, year, condition)
                        deal_score = score(price, value, hours, year, condition, end_date)
                        if title:
                            listings.append({
                                "id":              make_id("PurpleWave", lot_id or title),
                                "source_id":       lot_id,
                                "source_name":     "Purple Wave",
                                "source_url":      link,
                                "title":           title,
                                "current_bid":     price,
                                "location":        location,
                                "state":           location.split(",")[-1].strip()[:2] if "," in location else "",
                                "description":     title,
                                "image_url":       img,
                                "auction_end":     end_date,
                                "category":        category,
                                "asset_type":      "equipment",
                                "condition":       condition,
                                "hours":           hours,
                                "year":            year,
                                "score":           deal_score,
                                "estimated_value": value,
                                "scraped_at":      datetime.utcnow().isoformat(),
                            })
                except:
                    pass
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"Purple Wave error: {e}")
    return listings

# ─── SAVE TO SUPABASE ────────────────────────────────────
async def save_to_supabase(listings):
    if not SUPABASE_KEY:
        log.warning("No SUPABASE_KEY set — skipping save")
        return 0
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    saved = 0
    async with httpx.AsyncClient() as client:
        for listing in listings:
            try:
                resp = await client.post(
                    f"{SUPABASE_URL}/rest/v1/listings",
                    headers=headers,
                    json=listing,
                    timeout=10
                )
                if resp.status_code in (200, 201):
                    saved += 1
                else:
                    log.debug(f"Save failed: {resp.status_code} {resp.text[:100]}")
            except Exception as e:
                log.error(f"Save error: {e}")
    return saved

# ─── MAIN ────────────────────────────────────────────────
async def main():
    log.info("=" * 50)
    log.info("Deal Radar Scraper Starting")
    log.info(f"Time: {datetime.utcnow().isoformat()}")
    log.info("=" * 50)

    all_listings = []
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        # GovPlanet
        gp = await scrape_govplanet(client)
        log.info(f"GovPlanet: {len(gp)} listings")
        all_listings.extend(gp)

        # Purple Wave
        pw = await scrape_purplewave(client)
        log.info(f"Purple Wave: {len(pw)} listings")
        all_listings.extend(pw)

    log.info(f"Total scraped: {len(all_listings)}")
    hot = [l for l in all_listings if l["score"] >= 85]
    log.info(f"Hot deals (85+): {len(hot)}")

    saved = await save_to_supabase(all_listings)
    log.info(f"Saved to Supabase: {saved}")
    log.info("Done!")

    # Print sample
    for l in sorted(all_listings, key=lambda x: x["score"], reverse=True)[:3]:
        log.info(f"  [{l['score']}] {l['title']} — ${l['current_bid']:,.0f} (est ${l['estimated_value']:,.0f})")

if __name__ == "__main__":
    asyncio.run(main())
