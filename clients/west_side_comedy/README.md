# West Side Comedy Club — SMB Client #1

## Status
Pilot — free for 3 months. Goal: prove the engagement → conversion model.

## Business Details
- **Name:** West Side Comedy Club
- **Location:** New York, NY
- **Website:** https://www.westsidecomedyclub.com
- **Type:** Comedy Club
- **Slug:** `west_side_comedy`
- **Config:** `creator_config/west_side_comedy.json`

## Phone Numbers (fill in when confirmed with owner)
- **Business SMS number (subscribers text this):** TBD
- **Owner phone (they text this to send blasts):** TBD
- **SMS Keyword for signup:** TBD

## Pilot Success Metrics
- X% of subscribers respond to at least one weekly message in month 1
- Y seats filled via availability blast in 3 months

## How the Owner Sends a Blast
The owner texts the business number with something like:
> "Seats available tonight at 8pm — 20% off"

The bot detects it's an owner message, parses it as a blast, and sends it to relevant subscribers.

## Subscriber Signup Flow
1. Customer texts the signup keyword to the business number
2. Bot asks 2 preference questions (comedy type, frequency preference)
3. Preferences saved — subscriber is enrolled
4. First weekly message goes out on the next scheduled send

## Environment Variables (set in Railway when ready)
- `SMB_WEST_SIDE_COMEDY_OWNER_PHONE`
- `SMB_WEST_SIDE_COMEDY_SMS_NUMBER`
- `SMB_WEST_SIDE_COMEDY_KEYWORD`
- `SMB_WEST_SIDE_COMEDY_PORTAL_TOKEN` — random secret that acts as the magic-link token

## Client Portal
Once `SMB_WEST_SIDE_COMEDY_PORTAL_TOKEN` is set in Railway, share this URL with the owner:

```
https://<your-railway-domain>/portal/west_side_comedy?token=<token>
```

The portal shows:
- Active subscriber count and onboarding funnel
- Full blast history (date, message, delivery rate)
- Breakdown of audience preferences from sign-up questions

No login required — the token in the URL is the auth. Rotate the env var to invalidate the link.
