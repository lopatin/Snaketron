#!/bin/bash

# Test script for rate limiting on username check endpoint

echo "Testing rate limiting on /api/auth/check-username endpoint..."
echo "This test will make 12 requests to check if rate limiting kicks in after 10 requests."
echo ""

API_URL="http://localhost:3000/api/auth/check-username"

# Make 12 requests
for i in {1..12}; do
    echo "Request $i:"
    response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST "$API_URL" \
        -H "Content-Type: application/json" \
        -H "X-Real-IP: 192.168.1.100" \
        -d "{\"username\": \"testuser$i\"}")
    
    # Extract HTTP status code
    http_status=$(echo "$response" | grep "HTTP_STATUS" | cut -d: -f2)
    body=$(echo "$response" | grep -v "HTTP_STATUS")
    
    echo "Status: $http_status"
    echo "Response: $body"
    
    if [ "$http_status" = "429" ]; then
        echo "✓ Rate limiting is working! Request $i was rejected with status 429 (Too Many Requests)"
    elif [ "$i" -gt 10 ] && [ "$http_status" = "200" ]; then
        echo "✗ Rate limiting not working properly. Request $i should have been rate limited."
    fi
    
    echo "---"
done

echo ""
echo "Test complete!"
echo ""
echo "Expected behavior:"
echo "- Requests 1-10 should return status 200 (OK)"
echo "- Requests 11-12 should return status 429 (Too Many Requests)"