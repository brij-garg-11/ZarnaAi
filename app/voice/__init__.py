"""
Inbound phone-voice service for the Zarna AI platform.

This package is a SEPARATE Railway service (entry point: voice_main.py) that
answers phone calls via Twilio Programmable Voice + ConversationRelay and speaks
back in the creator's ElevenLabs voice clone. It reuses the existing AI brain
(app.brain) for reply generation but never touches the live SMS pipeline in
main.py.

Flow:
    Fan calls the Twilio number
      -> Twilio fetches TwiML from POST /twilio/voice
      -> TwiML <Connect><ConversationRelay> opens a WebSocket to /voice/relay
      -> ConversationRelay does streaming STT + ElevenLabs TTS; our WS server
         only exchanges TEXT (transcribed prompt in, reply text out)
      -> reply text is produced by app.voice.voice_brain (channel="voice")
"""
