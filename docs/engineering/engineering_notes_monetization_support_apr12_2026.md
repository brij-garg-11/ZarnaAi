# Engineering Notes: Supporting the Monetization Plan

Date: April 12, 2026

## Purpose

This document translates the monetization strategy into engineering work. The goal is to define what the product and data systems need in order to support a repeatable revenue engine for Zarna, and later a SaaS product for comedians.

This is not a full implementation spec. It is an engineering planning note focused on:

- what already exists,
- what is missing,
- what should be built first,
- and how to phase the work.

## Product Goal

Support a system that can do three things reliably:

1. Identify which fans are most likely to buy tickets in a given city.
2. Identify which fans are most likely to buy premium offers.
3. Measure whether a campaign actually drove revenue.

If the product cannot do those three things, it cannot support the monetization plan well.

## What Already Exists

Based on the current codebase, the system already has useful building blocks:

- `contacts` table with `source`, `fan_tags`, and `fan_location`
- `messages` table with rich engagement analytics fields
- tracked links and click logging
- blast infrastructure
- session analytics
- live show signup tables
- admin analytics for reply rate, tone, intent, and session depth

This means the team does **not** need to start from zero. The current system already supports:

- location-based audience targeting
- tag-based audience targeting
- campaign link tracking
- click measurement
- session-depth measurement
- repeated conversational engagement analysis

## Main Gaps

### 1. No true purchase attribution

This is the biggest gap.

Right now the system appears able to measure:

- who received a message,
- who clicked a tracked link,
- who replied,
- and who engaged deeply.

But it does not appear able to measure:

- who actually bought a ticket,
- who bought premium seats,
- who bought meet-and-greet,
- or who bought merch.

Without this, the system can optimize for clicks and engagement, but not confidently for revenue.

### 2. No explicit fan scoring layer

There is strong underlying behavior data, but it does not appear packaged into a clear reusable score such as:

- ticket-buy likelihood
- premium-buy likelihood
- superfan likelihood

This needs to exist as a first-class product concept, not just something inferred mentally from dashboards.

### 3. Audience segmentation is too primitive

The current system appears to support audience selection by broad types such as location, tag, or live-show signup. That is useful but not enough.

The monetization plan needs productized segments such as:

- likely buyer in city
- likely premium buyer
- recent clicker
- repeat replier
- deep-conversation fan
- dormant local fan
- superfan

### 4. No structured show-revenue model

The system knows about live shows and signups, but it does not yet appear to treat each show as a revenue object with funnel metrics such as:

- reachable audience
- clicks
- claimed purchases
- estimated purchases
- premium interest
- merch revenue
- total attributed revenue

### 5. Merch and premium revenue are not connected well enough

Since merch is sold in person, and premium experiences may not be reconciled fan-by-fan in the system, there is no clean lifetime value loop yet.

That means the product cannot yet answer:

- who spends the most,
- who upgrades often,
- who buys across multiple offers,
- and who should receive higher-value offers next.

## Engineering Workstreams

## Workstream 1: Attribution Layer

This should be the highest priority.

### Goal

Create the best available show-level and fan-level purchase attribution, even if it starts imperfectly.

### Minimum version

Build a practical attribution system around:

- unique tracked links per show and campaign
- unique tracked links for premium offers
- optional campaign codes
- post-purchase self-report prompts
- manual reconciliation fields

### Recommended data model additions

Potential new tables or fields:

- `show_campaigns`
- `show_campaign_recipients`
- `show_conversions`
- `fan_purchases`
- `purchase_source`
- `purchase_type`
- `purchase_value`
- `attribution_confidence`
- `claimed_via_sms`

### What this unlocks

- revenue per campaign
- revenue per segment
- revenue per targeted fan
- better comparison across shows and cities

## Workstream 2: Fan Scoring

### Goal

Create reusable fan-level scores that power targeting and reporting.

### First scores to build

1. `ticket_intent_score`
2. `premium_intent_score`
3. `superfan_score`

### First-pass inputs

These inputs appear feasible with current data:

- location matches active show city/region
- tracked-link clicks
- number of sessions
- recent reply activity
- deep session history
- returned within 7 days
- past live-show signup
- message frequency
- reply rate to blasts or offers

### Product requirement

Scores should be visible and usable in three places:

- admin fan profile or fan list
- campaign audience builder
- reporting dashboards

### Important note

The first scoring model does not need ML. A weighted rules-based score is good enough to start.

## Workstream 3: Revenue Segmentation

### Goal

Make audience targeting match monetization needs rather than just broad demographics.

### Needed segment types

- `hot_city_buyers`
- `premium_candidates`
- `superfans`
- `recent_clickers`
- `repeat_repliers`
- `deep_session_fans`
- `dormant_locals`

### Product work needed

- saved segment definitions
- dynamic segment refresh
- segment preview counts
- ability to use a segment directly in blast creation
- ability to combine filters such as city + score + recent click behavior

### Suggested implementation path

Start with computed SQL-backed segments. Do not overbuild a general segmentation engine on day one.

## Workstream 4: Show Revenue Objects

### Goal

Treat each show as a monetization unit, not just a marketing event.

### Each show should have

- city and date metadata
- linked tracked links
- associated campaigns
- audience counts
- click counts
- premium interest counts
- manual notes
- attributed or estimated revenue

### Needed product surface

A show-level dashboard or section that answers:

- How many fans in this market can we reach?
- Which segments are strongest?
- Which campaigns have run?
- Which campaign drove the most value?
- Is this show underperforming relative to similar cities?

## Workstream 5: Premium and Merch Support

### Goal

Support upsells beyond base ticket sales.

### Needed capabilities

- campaign-level premium offer links
- premium interest tagging
- manual mark-as-sold workflows
- post-show merch capture workflows
- notes for concierge outreach

### Why this matters

Zarna already sells premium seats, meet-and-greet, and merch. The product should treat those as core revenue paths, not side effects.

## Workstream 6: Experimentation and Reporting

### Goal

Turn each show cycle into a measurable experiment.

### Needed capabilities

- campaign variant labels
- message-angle labels
- segment labels
- per-variant comparison
- simple experiment reporting

### Initial questions the dashboard should answer

- Did funny/personal framing outperform generic framing?
- Did premium framing increase total revenue?
- Did sending to a higher-intent segment first improve results?
- Did manual concierge follow-up outperform automation-only?

## Workstream 7: Admin Dashboard UI Changes

### Goal

Make the monetization system obvious and operational inside the admin dashboard, so the team can use it without mentally stitching together multiple tabs.

The UI should make three things extremely clear:

- who the highest-value fans are,
- what is happening for each show,
- and whether campaigns are making money.

### Recommended admin dashboard additions

Because the existing admin file is already large, new admin surfaces should be added as dedicated modules and tabs rather than making the current admin entrypoint even bigger.

### 1. Revenue Overview tab

Add a new admin tab focused on monetization, not just engagement.

It should show:

- total reachable audience
- total blast-eligible audience
- imported-only subscribers vs conversation-known subscribers
- fans in active show markets
- top revenue segments
- attributed or estimated revenue for recent shows
- premium and merch signals
- current sync/backfill health

This should become the main "money dashboard."

### 2. Segments tab

Add a dedicated segments UI that lists the new customer segments clearly.

It should show:

- segment name
- segment definition summary
- fan count
- recent click rate
- recent reply rate
- recent claimed purchase rate
- related active cities or shows
- quick action to send to segment or export/audit it

Initial segments to display explicitly:

- `hot_city_buyers`
- `premium_candidates`
- `superfans`
- `recent_clickers`
- `repeat_repliers`
- `deep_session_fans`
- `dormant_locals`
- `imported_only_subscribers`

### 3. Show Revenue tab

Add a show-level revenue dashboard for each active and recent show.

It should show:

- reachable audience in market
- segment breakdown within the market
- campaigns sent for the show
- clicks by campaign
- purchase claims
- premium signals
- merch notes or post-show capture
- attributed and estimated revenue
- comparison to similar recent shows

The show page should answer:

- Is this market strong or weak?
- Are we targeting the right fans?
- Which campaign is working?
- Do we need manual follow-up?

### 4. Blast composer upgrades

The blast UI should be upgraded so campaigns are built around shows and segments, not only generic blast drafts.

Add:

- required show association for show-related campaigns
- segment picker
- audience preview count by segment
- campaign type selector such as announcement, premium upsell, last call, concierge assist
- variant label
- tracked-link requirement for monetization campaigns
- post-send revenue summary

### 5. Fan profile UI upgrades

Each fan profile should become useful for monetization decisions.

Add:

- ticket intent score
- premium intent score
- superfan score
- segment memberships
- recent clicks
- recent campaign exposure
- claimed purchases
- manual notes
- imported-only vs conversation-known badge

This gives the operator a real fan CRM view.

### 6. Subscriber sync/backfill status card

Because thousands of subscribers are still outside the DB, the admin dashboard should make sync health very visible.

Add a dedicated sync status card or tab showing:

- total subscribers in SlickText
- total contacts in local DB
- imported-only contacts
- contacts with conversation history
- last successful sync time
- dedupe count across textwords
- newly imported count in last 24h / 7d
- sync errors or drift warnings

This avoids confusion around why blast audience numbers differ.

## Workstream 8: Weekly Tech Updates and Revenue Updates

### Goal

Ensure the monetization system shows up in the weekly operating rhythm, not just in the live admin UI.

### Recommendation

The current weekly AI quality digest is useful, but it is focused on conversation quality. The business now also needs a weekly revenue-oriented update.

There are two reasonable paths:

1. Extend the existing weekly digest flow to include a monetization section.
2. Create a separate weekly monetization digest and optionally sync it to Notion.

### What weekly updates should include

Every weekly tech or operating update should include:

- total subscribers
- imported-only subscribers
- blast-eligible subscribers
- active fans in upcoming show markets
- top-performing segments this week
- show-by-show clicks and purchase claims
- premium intent signals
- merch capture notes if available
- sync/backfill health
- attribution confidence notes
- engineering issues blocking monetization

### Suggested weekly sections

- `Audience health`
- `Revenue funnel`
- `Top segments`
- `Show performance`
- `Backfill/sync health`
- `Engineering blockers`

### Recommended implementation

The simplest path is likely:

- keep the existing AI quality digest focused on AI reply quality,
- add a new weekly monetization digest script,
- save its results to the DB,
- and optionally push a Notion page using the same pattern as the current digest and show sync jobs.

This avoids mixing AI-quality reporting and monetization reporting into one confusing artifact.

## Workstream 9: SlickText Backfill and Ongoing Sync

### Goal

Bring the thousands of currently missing subscribers into the local system so monetization workflows can target the real audience, not just people who have already texted the bot.

### Why this matters

Right now there is a gap between:

- people subscribed in SlickText,
- and people represented as contacts in the local DB.

That gap weakens:

- blast reach,
- audience counts,
- city targeting,
- revenue segmentation,
- and reporting trust.

### Backfill requirements

The import job should:

- pull all active subscribers from SlickText
- merge across both textwords
- deduplicate by phone number
- upsert into `contacts`
- preserve source provenance
- mark imported contacts that have never texted the bot
- keep the process idempotent

### Recommended contact model additions

Potential additions to `contacts` or related tables:

- `external_subscriber_id`
- `provider`
- `provider_list_id`
- `is_imported_only`
- `is_blast_eligible`
- `first_seen_in_provider_at`
- `last_synced_at`
- `sync_source`
- `subscriber_status`

If one contact belongs to multiple textwords, that relationship may be cleaner in a separate mapping table rather than flattening it awkwardly.

### Recommended import flow

1. Fetch active subscribers from all relevant SlickText lists.
2. Normalize phone numbers.
3. Deduplicate across textwords.
4. Upsert into local contacts.
5. Preserve existing contact memory, tags, and conversation-linked fields.
6. Mark contacts with no inbound history as `imported_only`.
7. Record sync stats and errors.

### Ongoing sync

This should not be a one-time backfill only. It should become a recurring sync job.

Recommended cadence:

- one full backfill/import immediately
- then scheduled recurring sync, ideally daily
- plus an admin-triggered manual sync button for debugging or urgent campaign prep

### UI requirements for backfilled subscribers

Backfilled subscribers should be visible in the admin dashboard as a distinct audience, not silently mixed in a way that causes confusion.

The UI should clearly indicate:

- imported-only subscriber
- has texted the bot
- has clicked links
- eligible for monetization targeting
- missing location or enrichment fields

### Reporting requirements for sync

Weekly updates and dashboard reporting should include:

- how many subscribers exist only in SlickText vs now imported locally
- how many were newly backfilled this week
- how many remain unmatched or invalid
- whether sync drift exists between provider counts and local counts

### Important behavior rule

Backfilled contacts should become targetable for monetization campaigns, but the product should be careful not to treat imported-only fans as if they are already highly engaged conversational fans.

That means:

- they should be included in blast reach,
- but scored separately until they show stronger engagement,
- and they should appear in their own segment such as `imported_only_subscribers`.

## Suggested Data Model Direction

This is not final schema design, but the product likely needs objects similar to:

- `fans`
  Existing `contacts`, enriched with scores and monetization properties

- `shows`
  Existing live shows, expanded with sales and campaign metadata

- `campaigns`
  Show-linked outbound efforts with audience, variant, link, and timing metadata

- `fan_events`
  Clicked, replied, claimed purchase, bought premium, attended, bought merch

- `purchases`
  Ticket, premium, meet-and-greet, merch, with value and confidence

- `segments`
  Saved or computed revenue-relevant groups of fans

- `subscriber_sources`
  Optional mapping table for one fan belonging to multiple provider lists or textwords

## Recommended Phasing

## Phase 1: Fastest path to usable revenue ops

Build first:

- show-specific tracked links
- campaign records linked to shows
- manual purchase-claim workflow
- first-pass fan scores
- first revenue-oriented segments
- initial SlickText backfill
- sync status UI

This phase should let the team run better campaigns immediately.

## Phase 2: Make the system decision-useful

Build next:

- show dashboard
- segment-aware blast creation
- campaign comparison reporting
- premium intent workflows
- manual reconciliation tools
- dedicated revenue overview tab
- segment explorer tab
- weekly monetization digest

This phase should let the team understand what is working.

## Phase 3: Tighten attribution and lifetime value

Build later:

- better downstream purchase matching
- fan-level purchase history
- merch and premium reconciliation
- repeat buyer identification
- LTV and retention reporting
- more automated sync reconciliation and provider imports

This phase turns the system from campaign tooling into a revenue CRM.

## Specific Engineering Questions To Resolve

1. Can Ticketmaster or venue exports be imported manually or automatically?
2. Can current blast objects be linked directly to shows without awkward retrofits?
3. What is the cleanest way to store fan-level purchase confidence when purchase data is only partial?
4. Should fan scores be persisted in the database, computed on demand, or both?
5. Where should segments live: SQL views, materialized tables, or app-layer logic?
6. Should premium and merch events be recorded on the fan profile directly or through a generic purchases/events table?
7. Should imported-only subscribers live in `contacts` directly or in a provider-staging table before promotion?
8. Should weekly monetization updates extend the current digest pipeline or use a separate report job?

## Suggested First Deliverables

If engineering starts this work now, the best first deliverables are probably:

1. `show campaign tracking`
   A way to associate every outbound push with a specific show and tracked link.

2. `fan scoring v1`
   A simple rules-based score for ticket intent, premium intent, and superfan likelihood.

3. `revenue segments`
   Computed audiences that can be used directly in campaigns.

4. `purchase claim capture`
   A lightweight workflow for fans to confirm ticket purchase by reply or tagged interaction.

5. `show performance dashboard`
   A simple internal view of reachable audience, clicks, claims, premium signals, and estimated revenue.

6. `subscriber backfill + sync v1`
   A one-time import plus recurring sync for the full SlickText audience, with drift reporting in admin.

7. `admin revenue UI`
   A clear revenue overview with segments, show performance, and sync health visible in one place.

## What Not To Overbuild Yet

Avoid spending early cycles on:

- a fully generic CRM platform
- complex machine-learning models
- overly flexible no-code segmentation builders
- polished multi-client SaaS architecture

The first goal is to make Zarna's show monetization engine work. Product generalization should follow proof.

## Summary

Engineering does not need to invent a monetization system from scratch. The current product already has strong engagement tracking and campaign primitives.

What is missing is the revenue layer:

- attribution,
- scoring,
- monetization-focused segmentation,
- and show-level revenue reporting.

If those pieces are built in the right order, the system can support the direct Zarna business first, and later become the backbone of a comedian touring SaaS product.
