from pathlib import Path

from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    pipeline,
)

MODEL_ID = "distilbert-base-uncased-finetuned-sst-2-english"
# Use local cache to avoid re-downloading if files are already present.
CACHE_DIR = Path(__file__).resolve().parents[1] / "models" / "distilbert-sst2"


def run_demo() -> None:
  """Load the pretrained sentiment model and run a quick sanity check."""
  CACHE_DIR.mkdir(parents=True, exist_ok=True)

  tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, cache_dir=CACHE_DIR)
  model = AutoModelForSequenceClassification.from_pretrained(
      MODEL_ID, cache_dir=CACHE_DIR
  )
  classifier = pipeline(
      task="sentiment-analysis",
      model=model,
      tokenizer=tokenizer,
      device="cpu",
  )

  samples = [
      "Bitcoin is surging to new highs!",
      "The market looks uncertain and people are nervous.",
  ]
  outputs = classifier(samples)
  for text, pred in zip(samples, outputs):
    print(f"{pred['label']:8} | score={pred['score']:.4f} | {text}")


if __name__ == "__main__":
  run_demo()
