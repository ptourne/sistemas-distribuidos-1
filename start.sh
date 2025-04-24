
# TODO: if ./joiner_credits exists, rm Rs
rm -R ./joiner_credits
rm -R ./joiner_ratings
mkdir ./joiner_credits
mkdir ./joiner_ratings
docker compose up -d --build
