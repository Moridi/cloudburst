ps -aux | grep "cloudburst/server/" | grep -v grep | awk '{print $2}' | xargs kill -9
rm *.pmemobj