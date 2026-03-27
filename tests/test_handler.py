import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.brain.handler import create_brain

PHONE = "+11234567890"


def test_handler():
    brain = create_brain()

    print("=== Zarna AI — Handler Test ===\n")

    messages = [
        "tell me a joke about Indian moms",
        "give me another one",          # tests conversation history carry-over
        "when is her next show?",
        "recommend a clip",
    ]

    for msg in messages:
        print(f"User:  {msg}")
        reply = brain.handle_incoming_message(PHONE, msg)
        print(f"Zarna: {reply}\n")

    # Verify conversation was saved correctly
    history = brain.storage.get_conversation_history(PHONE)
    assert len(history) == len(messages) * 2, (
        f"Expected {len(messages) * 2} messages in history, got {len(history)}"
    )
    assert history[0].role == "user"
    assert history[1].role == "assistant"
    print(f"✓ Conversation history saved correctly ({len(history)} messages)")

    # Verify contact was created
    contact = brain.storage.get_contact(PHONE)
    assert contact is not None
    assert contact.phone_number == PHONE
    print(f"✓ Contact saved: {contact.phone_number}")

    print("\nAll handler tests passed.")


if __name__ == "__main__":
    test_handler()
