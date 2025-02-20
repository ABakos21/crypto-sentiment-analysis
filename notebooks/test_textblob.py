from textblob import TextBlob

# Sample crypto news headlines
news_articles = [
    "Bitcoin surges past $60,000 as investors show strong demand.",
    "Ethereum crashes 10% after regulatory uncertainty.",
    "Crypto market remains stable despite economic downturn.",
    "Elon Musk tweets about Dogecoin, causing a price jump.",
    "Massive security breach leads to $100M loss in cryptocurrency exchange."
]

# Run sentiment analysis
for article in news_articles:
    blob = TextBlob(article)
    print(f"News: {article}")
    print(f"Polarity: {blob.sentiment.polarity:.2f} | Subjectivity: {blob.sentiment.subjectivity:.2f}\n")
