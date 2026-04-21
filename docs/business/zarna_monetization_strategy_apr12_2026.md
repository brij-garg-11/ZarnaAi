# Zarna Monetization Strategy

Date: April 12, 2026

## Objective

The goal for the next 90 days is not just "more revenue." The goal is to prove that Zarna's owned fan audience can be converted predictably into ticket, premium, and merch revenue, and that the playbook can later become a software product for comedians.

## Current Reality

What we know from the business:

- Zarna makes money directly from ticket sales.
- Average tickets are around $60, with better/closer seats priced materially higher.
- Existing revenue extensions already exist: merch, meet-and-greet, and premium seats.
- Ticket sales currently happen by sending fans to Ticketmaster links.
- Best current sales channels are Instagram and podcasts.
- Fans tend to buy immediately after announcement, then sales continue more slowly over time.
- Zarna often sells out, so the opportunity is not only top-of-funnel volume. It is also faster sell-through, better premium mix, better fan targeting, and higher revenue per fan.
- There are around 4-6 shows per month.
- Manual experimentation is acceptable. Same-day campaign changes are possible, and engineering bandwidth exists.
- Zarna enjoys sending funny, engaging messages and does not want to deceive or compromise fans.

What we know from the product/data stack:

- Contacts already store `source`, `fan_tags`, and `fan_location`.
- Message analytics already track reply outcomes, reply speed, link presence, link click behavior, intent, tone, routing tier, and conversation turn.
- Session analytics already exist, including session depth, duration, and whether a fan came back within 7 days.
- Tracked links already exist, and click behavior can already be measured.
- The system does not appear to have true ticket purchase attribution yet.
- There does not appear to be a literal `engagement_score` field in code; the product seems to derive engagement from message and session analytics rather than a single stored score.

## Core Thesis

Zarna does not have a data problem. She has a packaging and monetization problem.

The real asset is not "an AI chatbot." The real asset is:

- a high-response owned SMS audience,
- unusually strong first-party conversation data,
- city/location signal,
- link-click signal,
- session-depth signal,
- and the ability to identify fans with high affinity.

The near-term business is:

**Use fan data and conversational SMS to increase revenue per show, per targeted fan, and per announcement.**

The future business is:

**Turn the winning internal playbook into a SaaS product for touring comedians.**

## Monetization Model 1: Zarna Direct Revenue Engine

### Best First Customer Segment

The best first segment is touring fans.

This is the right starting point because:

- Zarna already monetizes live demand,
- purchase intent is tied to geography and timing,
- fans buy close to announcements,
- and the team can run fast experiments around each show.

### What the system should optimize

For each show, the product should improve:

- ticket conversion,
- premium seat conversion,
- meet-and-greet attach rate,
- merch attach rate,
- speed of sell-through,
- and eventually repeat purchase behavior.

### Fan Segments

Build the revenue system around four fan segments:

1. `hot_city_buyers`
   Fans in or near the market for an announced show.

2. `premium_candidates`
   Fans who click quickly, reply often, engage personally, or show above-average affinity.

3. `superfans`
   Fans with repeat replies, deeper sessions, higher session frequency, or strong click and conversation behavior.

4. `warm_national_fans`
   Fans who are not near an active show but are worth nurturing until a relevant city, product, or launch appears.

### Offer by Segment

- `hot_city_buyers`: ticket purchase
- `premium_candidates`: better seats, meet-and-greet, priority access
- `superfans`: merch, limited drops, exclusive clips, voice notes, behind-the-scenes access, future membership
- `warm_national_fans`: engagement and nurture until a live offer becomes relevant

### Revenue Formula

The key operating formula is:

**Revenue per targeted fan = ticket conversion + premium upsell + merch attach**

That means the system has to answer:

- who gets the first announcement,
- who gets urgency follow-up,
- who gets a premium offer,
- who gets a concierge-style manual follow-up,
- and who should not be messaged as aggressively.

## 90-Day Operating Plan

### Phase 1: Build a basic show revenue engine

For each show, create simple working segments using the data already available:

- city/location match
- clicked ticket links before
- repeated replier
- deep session fan
- high-affinity responder
- premium candidate

Then run a simple campaign stack:

1. Announcement blast
   City-targeted, funny, direct Ticketmaster CTA.

2. Clicker follow-up
   Follow up with people who clicked but have not shown a stronger downstream signal.

3. Premium upsell
   Send only to higher-intent fans.

4. Concierge/manual pass
   Personally follow up with the top 25-100 most likely buyers or premium buyers.

5. Last-call message
   Short urgency push closer to show day.

Because same-day execution is possible and manual work is acceptable, this can be run immediately without waiting for a perfect system.

### Phase 2: Instrument what matters

The biggest blind spot is purchase attribution.

For every show, the team should track:

- fans targeted,
- unique clickers,
- click-through rate,
- reply rate,
- premium interest,
- claimed purchases,
- actual purchases if obtainable,
- merch revenue,
- meet-and-greet revenue,
- premium seat revenue,
- and total revenue per targeted fan.

If Ticketmaster does not expose purchase-level attribution cleanly, start with proxies:

- unique tracked links by city and campaign,
- dedicated VIP links or codes,
- post-click follow-up,
- post-purchase prompts like "reply YES if you bought tickets,"
- venue or promoter reconciliation where possible,
- and manual matching for a small subset of campaigns.

Without attribution, the team can still market. Without attribution, the team cannot prove predictable conversion well.

### Phase 3: Run repeatable experiments

The first test design should stay simple:

- generic city announcement vs funny/personal city announcement
- ticket-only CTA vs ticket + premium framing
- send to all city fans vs send to high-intent city fans first
- automated blast only vs blast + manual concierge follow-up

The key question is:

**Which segment + message + offer combination produces the most revenue per targeted fan?**

## Product Requirements for the Internal Zarna System

The internal product does not need to be complicated. It needs to be useful.

It should answer questions like:

- Which fans are most likely to buy for this city?
- Which fans are most likely to buy premium seats?
- Which fans deserve personal follow-up?
- Which message angle performs best by city or segment?
- Which shows are underperforming relative to similar markets?

If the system can answer those questions reliably, it is already valuable.

## Initial KPI Stack

Primary KPI:

- revenue per targeted fan by show/city

Secondary KPIs:

- ticket link CTR
- reply rate
- premium attach rate
- merch attach rate
- time to sell-through after announcement
- repeat purchase intent
- % of revenue coming from top-intent segments

## Product Requirements for Future Comedian SaaS

The later product should not be positioned as "AI chatbot for comedians."

That is too vague.

The better framing is:

**A touring revenue CRM for comedians.**

### Promise

Turn text subscribers into ticket buyers, premium buyers, and repeat superfans.

### Likely buyer

Independent comedians and small comedy teams with real touring demand, but weak CRM sophistication.

### Product modules

1. Fan scoring
   Estimate ticket-buy likelihood, premium likelihood, and superfan likelihood.

2. Show campaign builder
   Launch city-specific announcement, reminder, upsell, and last-call campaigns.

3. Revenue segmentation
   Separate ticket-only, premium, merch, and nurture audiences.

4. Attribution dashboard
   Show which campaigns, segments, and messages drove clicks and purchases.

5. Superfan workflows
   Identify the fans who should get early access, exclusive offers, or manual outreach.

### Likely pricing direction

The future business may look like SaaS, but value is strongly tied to revenue outcomes. A likely pricing path is:

- monthly software fee,
- with optional setup/onboarding,
- and possibly higher tiers for strategy or managed support.

A hybrid model may prove easier to sell initially than pure self-serve software.

## What the current system can already support

Based on the existing codebase, the current system can likely support these workflows now:

- audience targeting by `fan_location`
- audience targeting by `fan_tags`
- tracked campaign links
- click measurement
- reply-rate analysis
- intent and tone analysis
- session-depth and retention analysis
- identifying richer engagement behavior than a simple subscriber count

This is enough to begin running monetization experiments now, even before purchase attribution is perfect.

## Biggest Gaps To Close

1. Ticket purchase attribution
   This is the biggest gap between "interesting marketing" and "provable revenue engine."

2. Fan-level revenue history
   The system does not yet appear to know who bought which tickets, who purchased premium, or who bought merch.

3. Clear fan scoring layer
   There is strong behavioral data, but it does not yet appear to be packaged into one clear buyer-intent or superfan score.

4. Source-of-subscription clarity
   The system appears to store `source`, but current operating visibility may not yet cleanly distinguish organic, show, keyword, and campaign origin for every fan.

5. Online merch connection
   Merch is currently sold in person, which makes it harder to connect merch revenue to fan records.

## Immediate Next Steps

1. Define the first working fan score
   Even a lightweight score is enough to start:
   - location match to show
   - clicked show links before
   - replied in past 30 days
   - deeper session history
   - multiple sessions

2. Create a per-show experiment template
   Every show should follow the same campaign structure and measurement sheet.

3. Build a simple attribution workaround
   Use tracked links, VIP codes, and post-purchase self-reporting until better Ticketmaster/venue data is available.

4. Start segmenting premium buyers separately
   Since premium seats and meet-and-greet often exist, that should be an explicit branch of the funnel, not an afterthought.

5. Turn post-show behavior into future revenue
   Capture who attended, who bought premium, and who bought merch whenever possible so future campaigns improve over time.

## Unanswered Questions To Research Or Validate

- Whether Ticketmaster or venue partners can provide enough downstream purchase data to reconcile SMS-driven sales
- Whether premium and meet-and-greet inventory can be controlled tightly enough to run meaningful attach-rate experiments
- Whether the system already exposes enough fan-level analytics in the admin UI to create a first-pass buyer-intent score without additional backend work

## Summary

The right near-term business is not "monetize fan data" in the abstract.

The right near-term business is:

**Use Zarna's SMS audience and engagement data to increase revenue per show, per targeted fan, and per premium offer.**

Once that works repeatedly, the future SaaS for comedians becomes much easier to define and sell:

**A touring revenue CRM that turns text subscribers into buyers and superfans.**
