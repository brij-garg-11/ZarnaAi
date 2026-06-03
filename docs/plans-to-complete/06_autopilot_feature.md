# Plan: Auto-Pilot — AI-Managed Fan & Customer Engagement
_Created: May 1, 2026_
_Status: Brainstorm complete. Ready for Phase 1 scoping._

---

## What This Is

Auto-Pilot is a new category of feature — not SMS blast scheduling (competitors do that), but **agentic, proactive fan and customer relationship management** that runs in the background with the creator's permission.

The core insight: every competitor (Attentive, Klaviyo, Recart) auto-pilots **one-way e-commerce blasts**. We already have **two-way AI conversations in the creator's voice**. Auto-Pilot uses the signals from those conversations to proactively reach back out — personal, timely, and voice-matched — without the creator doing anything.

It works for both **creators** (performers, comedians, influencers) and **SMBs** (salons, restaurants, service businesses). Same engines, different configuration.

---

## The 5 Engines

### Engine 1: Conversation Follow-up Engine ⭐ (build first — highest moat)
Reads ongoing fan conversations, detects signals that warrant a future follow-up, and schedules them automatically.

**Triggers:**
- Fan mentions wanting to see a show / attend an event
- Fan shares something personal or emotional
- Fan expresses interest in merch, content, or an offer
- Superfan signal detected (deep loyalty language)
- Fan asked something the AI couldn't fully answer (unresolved need)
- Fan was highly engaged then suddenly went cold

**Routing rule (1:1 vs. segment):**
- Personal / emotional / superfan / unanswered → always 1:1, AI drafts from conversation history
- Show / merch interest → 1:1 if fewer than N fans triggered same signal; smart segment blast if many fans share it
- Cold fan → always 1:1 re-engagement

### Engine 2: Re-engagement Engine
Detects fans/customers who've gone cold and reaches back with a warm, personalized message — not a generic win-back blast. References what the fan said in their last conversation.

### Engine 3: Blast Engine
Based on fan data, creator goals, and calendar context, AI drafts and (with permission) sends blasts without the creator building them manually. Knows the voice, the audience, and the goal.

### Engine 4: Moment Engine
Event-based triggers fire automatically: show coming up (48h / 24h / day-of reminders), new content drop, fan milestones (anniversary of joining, Nth message).

### Engine 5: Optimization Engine
Tracks what works — which tones get replies, which times get clicks, which segments respond to which content — and adjusts future auto-pilot decisions. Auto-tests variants and promotes winners.

---

## Configuration UX

### Philosophy
Not a settings page. The mental model is **configuring a human assistant** — a short conversational wizard, then a structured plan the creator owns and can edit.

### Onboarding: Conversational Wizard (5 questions)

**For Creators:**
1. "What's the main goal for your fans right now?" _(open text — e.g. "I want them to come to my shows and feel like I actually know them")_
2. "What's coming up in the next 60 days I should know about?" _(shows, content drops, launches)_
3. "Anything you'd never want me to bring up?" _(optional)_
4. "How often is too often for one fan to hear from you?" _(gut feeling in plain English)_
5. "How involved do you want to be — review everything, or just see a weekly summary?" _(sets initial autonomy level)_

**For SMBs:**
1. "Tell me about your business — what do you do and who are your customers?" _(open text)_
2. "What's the one thing you most want more of?" _(repeat bookings, reviews, filling slow nights)_
3. "Do you have any patterns to your week — slow days, busy seasons, upcoming promos?" _(this is the most important question — feeds the business calendar)_
4. "How often is okay to reach out to a customer?" _(gut feeling)_
5. "Do you want to review messages before they go out, or trust me to handle it?" _(sets initial autonomy level)_

### The Plan Document

After the wizard, Auto-Pilot outputs a **structured rule-card document**. Design principle (informed by Tailscale/Formal.ai research): each rule is a **card** with a title, toggle (on/off), delete button, and plain-English text inside. This prevents the intimidation of a raw text file while preserving editability.

Each card type:
- **Goal card** — the creator's stated goal in their own words
- **Rule cards** — "When [trigger] → [action] after [timing]" — directly editable
- **Calendar card** (SMBs) — slow days, busy days, upcoming promos
- **Guardrail card** — frequency caps, quiet hours, auto-pause conditions

Editing: creator edits text directly in each card. If the edit is ambiguous, AI flags inline with a clarifying question. A "Text View" toggle shows the full plain-text plan for power users — both views stay in sync.

### Modes (Manual Switching)

Creator manually switches mode when their focus shifts. When they add a show in the operator dashboard, a gentle prompt asks: "Want to switch to Tour Mode?" — opt-in, not auto.

**Creator Modes:**
- `Stay Warm` (default) — fan relationships, check-ins, re-engagement
- `Tour / Show Mode` — show follow-ups lead, blast cadence increases
- `Launch Mode` — shaped around a content/merch release window
- `Re-engage Mode` — focused entirely on cold fans

**SMB Modes:**
- `Business as Usual` (default) — regular re-engagement rhythm
- `Promo Mode` — active push for a specific offer or event
- `Fill-the-Gaps Mode` — targeted at specific slow time slots
- `Win-Back Mode` — focused on customers lapsed 60+ days

### Oversight System

Three layers, all three always available:
1. **Weekly Digest** — Monday email/SMS to creator: what happened, what's planned, one-click pause if anything looks off
2. **Live Dashboard** — Auto-Pilot tab in operator dashboard with every queued/in-flight/sent action, fan-level drill-down, performance by signal type, per-engine pause controls
3. **Real-Time Alerts** — notifications for high-risk sends (emotional check-ins), unusual activity (opt-out spike), and milestone hits

### Autonomy: Trust Escalation Model
- Starts in **draft-only** mode — everything goes to review queue
- As creator approves actions without editing, the AI earns incremental autonomy
- Low-risk sends (show follow-ups, re-engagement after 30 days) graduate to auto-send first
- High-risk sends (emotional check-ins, unanswered needs) always stay gated in V1
- Creator can manually adjust autonomy level per engine at any time

---

## Non-Negotiable Guardrails (must ship before any auto-send)

| Guardrail | Behavior | Priority |
|---|---|---|
| Frequency cap | Max N messages per fan per week (creator-configurable) | Critical |
| Quiet hours | TCPA-compliant 9am–9pm local timezone per fan | Critical |
| Opt-out protection | Any STOP / opt-out immediately suppresses all auto-pilot sends for that fan | Critical |
| Tone guardrails | AI stays in approved tone modes only, never goes off-plan | High |
| Draft gate on first N sends | Every new rule's first N sends go to review regardless of autonomy level | High |
| Auto-pause trigger | Reply rate or opt-out rate drops below threshold → pause + alert creator | High |
| Spend cap | Max SMS spend per month per creator plan (scales with plan tier) | Medium |
| Human override | Creator can pause/edit/cancel any pending send at any time | Critical |

---

## Phased Roadmap

### Phase 1 — Signal Detection & Follow-up Queue
**What ships:** The AI reads conversations in real-time and tags follow-up opportunities. A new "Follow-up Queue" tab appears in the operator dashboard. Creator sees a list of fans who have triggered a signal, with the signal type, the relevant conversation excerpt, and a suggested action. No auto-sends.

**Why this first:** Delivers standalone value immediately (creators can see which fans need follow-up without building Auto-Pilot). De-risks the core AI capability (signal extraction) before connecting it to any send pipeline. Fast to ship.

**Steps to build:**
- [ ] Design `fan_followup_signals` table: `fan_id`, `creator_slug`, `signal_type` (enum), `conversation_excerpt`, `suggested_action`, `status` (pending / actioned / dismissed), `triggered_at`, `actioned_at`
- [ ] Build `SignalExtractor` — a post-reply hook that runs after every AI conversation response and checks for trigger signals using intent classification (reuse existing `app/brain/` intent layer)
- [ ] Define signal detection prompts for each of the 6 signal types (show interest, emotional, superfan, merch interest, cold, unanswered)
- [ ] Store signals in `fan_followup_signals` with deduplication (don't re-flag the same fan for the same signal type within a cooldown window)
- [ ] Build Follow-up Queue UI in operator dashboard: list view by signal type, fan name, excerpt, suggested action, "Dismiss" and "Act on this" buttons
- [ ] "Act on this" → opens a draft message composer pre-filled with AI-drafted follow-up text in creator voice, contextualised to the specific conversation
- [ ] Add signal detection to `app/smb/` conversation handler as well (SMB parity)

**Est. effort:** 3–4 weeks

---

### Phase 2 — AI-Drafted Follow-ups with One-Click Approve
**What ships:** For every signal in the queue, Auto-Pilot pre-drafts the actual follow-up message (personalized, in creator voice, referencing conversation context). Creator reviews and one-click approves to send immediately or schedule. This is Auto-Pilot in draft-only mode.

**Steps to build:**
- [ ] `DraftGenerator` — for each new signal, auto-generate a draft message using RAG (creator voice) + conversation excerpt as context
- [ ] Store draft in `blast_drafts` table (re-use existing table, add `autopilot_signal_id` FK and `autopilot_draft = true` flag)
- [ ] Update Follow-up Queue UI: each item shows the AI-drafted message, editable inline before approving
- [ ] One-click "Approve & Send" → queues immediate send via existing blast/message pipeline
- [ ] One-click "Approve & Schedule" → lets creator pick timing, queues in scheduler
- [ ] Track approval rate, edit rate, and dismiss rate per signal type (needed for Phase 4 optimization)
- [ ] Weekly Digest email (V1) — simple weekly summary of signals detected, drafts approved/dismissed, sends completed

**Est. effort:** 2–3 weeks

---

### Phase 3 — Trigger-to-Send Pipeline (Supervised Auto-Send)
**What ships:** Specific low-risk trigger types graduate from draft-only to supervised auto-send. The re-engagement engine becomes its own cron. Show follow-ups fire automatically when a show is announced. High-risk signals (emotional, unanswered) remain review-gated.

**Steps to build:**
- [ ] Autonomy config per signal type: `draft_only` / `auto_send` / `auto_send_with_notification` — stored in `bot_configs` (or new `autopilot_config` table)
- [ ] Trust escalation tracking: `autopilot_approval_streak` per signal type — increments on every approved-without-edit; triggers autonomy upgrade prompt at threshold (e.g. 5 in a row)
- [ ] Connect show announcement (new show saved in `live_shows`) → check `fan_followup_signals` for show-interest fans in that city → auto-draft + schedule follow-up
- [ ] Re-engagement engine as standalone cron (replace `drip_reengagement.py` with full Auto-Pilot re-engagement): detects cold fans, drafts 1:1 message from conversation history, sends automatically if autonomy is set
- [ ] Auto-pause logic: if opt-out rate for auto-pilot sends exceeds threshold in any rolling 7-day window → pause all auto-sends for that creator + notify
- [ ] Real-time alerts: notify creator when any auto-send fires (configurable — can mute for low-risk sends after N consecutive successful ones)
- [ ] Live Auto-Pilot Dashboard tab in operator: in-flight queue, sent history, performance stats by signal type, per-engine pause toggle
- [ ] Guardrails enforcement: frequency cap checker runs before every auto-send; quiet hours enforced by scheduler

**Est. effort:** 3–4 weeks

---

### Phase 4 — Full Auto-Pilot
**What ships:** All 5 engines running. Goal-based configuration via the conversational wizard. The structured rule-card Plan document. Modes. The full oversight system. Optimization engine begins tracking performance and adjusting.

**Steps to build:**
- [ ] Conversational wizard UI (5-question flow) for both creator and SMB paths — saves output to `autopilot_config`
- [ ] Plan document generator — converts wizard answers to structured rule cards
- [ ] Rule card editor UI — editable plain-English text in each card, AI ambiguity flag, Text View toggle
- [ ] Mode switcher UI — dropdown in dashboard header, mode change updates active rule set
- [ ] Mode-aware prompt injection: show announcement → suggest Tour Mode prompt
- [ ] Blast Engine: AI selects blast topics/timing based on active mode + fan data + plan goal (generates draft, routes through existing blast pipeline)
- [ ] Moment Engine: fan milestone detection (join anniversary, Nth message), content drop hook (creator triggers via dashboard), show reminders (24h / day-of)
- [ ] Optimization Engine V1: per-signal-type performance dashboard (reply rate, opt-out rate, click rate) — inform creator, no auto-adjustment yet
- [ ] Optimization Engine V2: AI uses performance data to adjust timing and tone in future drafts automatically
- [ ] SMB business calendar input: slow-day selector, upcoming promo list — feeds into re-engagement timing logic
- [ ] 1:1 vs. segment routing: threshold config (how many fans with same signal = segment blast vs. individual 1:1s)
- [ ] Full weekly digest email with plain-English summary, upcoming actions, performance highlights, one-click pause

**Est. effort:** 4–6 weeks

---

## Testing Approach

### Phase 1 Testing
- **Unit:** Signal extractor correctly tags each of the 6 signal types from conversation fixtures; deduplication cooldown prevents re-flagging within window
- **Integration:** End-to-end: send a test message with show-interest language → signal appears in Follow-up Queue within N seconds
- **Manual QA:** Run against 20 real fan conversations across different signal types; review false positive / false negative rate
- **Acceptance:** Signal extractor precision > 85% on held-out conversation test set before shipping to any real creator

### Phase 2 Testing
- **Draft quality:** For each signal type, review 10 AI-drafted follow-up messages for voice accuracy, personalization quality, and appropriateness — use existing quality rubric from `ai_quality_reports`
- **Integration:** Approve a draft → confirm message hits the send pipeline and lands in `blast_drafts` / `blast_recipients` correctly
- **Edge cases:** Creator has no recent conversation context → draft gracefully handles sparse data; fan opted out → draft is suppressed
- **Acceptance:** Approval rate (creator approves without editing) > 60% in first 2 weeks of use — this measures whether drafts are good enough to save time

### Phase 3 Testing
- **Guardrail stress tests:** Frequency cap — verify fan cannot receive more than cap regardless of how many signals fire simultaneously; quiet hours — fire a scheduled send at 11pm, confirm it reschedules to morning; opt-out protection — mark fan as opted out, verify all pending auto-sends are suppressed immediately
- **Auto-pause test:** Simulate opt-out spike → confirm all auto-sends pause and creator is notified within 1 minute
- **Integration:** Show announced in city X → fans with show-interest signal in city X receive correctly-timed follow-up
- **Load:** 500 simultaneous signals processed without queue backup or duplicate sends
- **Acceptance:** Zero sends to opted-out fans in any test scenario. Zero sends outside quiet hours.

### Phase 4 Testing
- **Wizard output quality:** Wizard responses from 5 different creator personas → generated Plans are accurate, complete, and reflect stated goal
- **Plan edit roundtrip:** Edit a rule card text → verify AI interprets change correctly; edit something ambiguous → verify AI flags it inline
- **Mode switching:** Switch from Stay Warm to Tour Mode → verify correct rules activate/deactivate; show announcement prompt appears correctly
- **Optimization accuracy:** After 30 days of data, optimization engine's timing recommendations vs. actual reply rate data — verify recommendations directionally correct
- **Full E2E:** New creator completes wizard, plan is generated, one signal fires, draft is approved, message sends, reply comes back, signal detected in reply, new follow-up queued — full loop works end-to-end

---

## Success Metrics

| Metric | Target | When to Measure |
|---|---|---|
| Signal precision (correct signal type tagged) | > 85% | Phase 1 |
| Draft approval rate (approved without editing) | > 60% | Phase 2 |
| Creator time saved per week | > 30 min | Phase 2 |
| Auto-pilot reply rate vs. manual blast reply rate | Auto-pilot ≥ 1.5× manual | Phase 3 |
| Opt-out rate on auto-pilot sends vs. manual | Auto-pilot ≤ manual | Phase 3 |
| Zero sends to opted-out fans | 100% compliance | Every phase |
| Creator retention on Auto-Pilot plan tier | > 70% monthly | Phase 4 |
| Fan engagement uplift (reply rate before vs. after Auto-Pilot) | > 20% increase | Phase 4 |

---

## Open Questions (to resolve before building)

1. What is the 1:1 vs. segment blast threshold? (How many fans with same signal before it becomes a blast?)
2. How long should the AI wait before sending an emotional check-in? (2 days? 5 days? Creator-configurable?)
3. What is the hard frequency cap default? (2×/week per fan? 1×/week?)
4. How does trust escalation work exactly — how many consecutive approved-without-edit actions triggers an autonomy upgrade prompt?
5. Should the creator be able to see the specific conversation excerpt that triggered each follow-up?
6. For SMBs: is the business calendar a structured widget (day toggles + promo list) or free-form text input like the creator goal?
7. What is the monetization tie-in — how does Auto-Pilot capacity scale with plan tier? (Number of active rules? Number of auto-sends per month?)

---

## Notes on What's Already Built (Don't Rebuild)

- `drip_reengagement.py` — Phase 3 re-engagement engine replaces this, don't maintain both
- `blast_drafts` table — reuse for auto-pilot drafts with `autopilot_signal_id` FK and flag
- Intent classification in `app/brain/` — reuse for signal detection, don't build new classifier
- `fan_tags`, `fan_memory` — primary signal context for draft generation, no new storage needed
- Existing scheduler in `operator/app/scheduler.py` — extend for auto-pilot cron jobs
- `ai_quality_reports` — reuse rubric for draft quality assessment
