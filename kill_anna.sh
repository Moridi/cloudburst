ps -aux | grep "anna" | grep -v grep | awk '{print $2}' | xargs kill -9