from ntscraper import Nitter

scraper = Nitter(log_level=1, skip_instance_check=True, instances=["http://raspberrypi.local:8081"])

tweet = scraper.get_tweet_by_id("DystopiaPro69", "1971701333502967836", instance="http://raspberrypi.local:8081")

print(tweet)

