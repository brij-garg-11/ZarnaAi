import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.storage.memory import InMemoryStorage

def test_storage():
    db = InMemoryStorage()

    # save a new contact
    contact = db.save_contact("+11234567890", source="sms")
    assert contact.phone_number == "+11234567890"
    assert contact.source == "sms"
    print("✓ save_contact works")

    # saving same number again should not duplicate
    same = db.save_contact("+11234567890", source="qr")
    assert same is contact
    print("✓ duplicate contact not created")

    # get_contact
    found = db.get_contact("+11234567890")
    assert found is contact
    missing = db.get_contact("+19999999999")
    assert missing is None
    print("✓ get_contact works")

    # save messages
    db.save_message("+11234567890", "user", "give me a joke")
    db.save_message("+11234567890", "assistant", "My husband thinks he helps with the kids...")
    db.save_message("+11234567890", "user", "another one")

    history = db.get_conversation_history("+11234567890")
    assert len(history) == 3
    assert history[0].role == "user"
    assert history[1].role == "assistant"
    print("✓ save_message + get_conversation_history works")

    # limit respected
    short = db.get_conversation_history("+11234567890", limit=2)
    assert len(short) == 2
    assert short[0].text == "My husband thinks he helps with the kids..."
    print("✓ history limit works")

    print("\nAll storage tests passed.")

if __name__ == "__main__":
    test_storage()
