#!/bin/bash
# Complete Flask restart with verification

cd ~/projects/mystoreofvalue.com

echo "=== Step 1: Stop ALL Python processes on port 5000 ==="
# Kill by port
sudo fuser -k 5000/tcp 2>/dev/null
sleep 2

# Kill by process name
sudo pkill -9 -f "python.*api.py"
sleep 2

# Verify nothing is running
if pgrep -f "python.*api.py" > /dev/null; then
    echo "✗ Process still running, force killing..."
    sudo kill -9 $(pgrep -f "python.*api.py")
    sleep 2
else
    echo "✓ All processes stopped"
fi

echo ""
echo "=== Step 2: Check for syntax errors ==="
python3 -c "import py_compile; py_compile.compile('api.py', doraise=True)" 2>&1
if [ $? -eq 0 ]; then
    echo "✓ No syntax errors"
else
    echo "✗ Syntax error found! Fix before continuing."
    exit 1
fi

echo ""
echo "=== Step 3: Test imports ==="
python3 << 'PYEOF'
import sys
import os
sys.path.insert(0, os.getcwd())
try:
    import api
    print("✓ api.py imports successfully")
    print(f"✓ Routes registered: {len(list(api.app.url_map.iter_rules()))}")
    
    # Check for comment routes
    has_comments = False
    for rule in api.app.url_map.iter_rules():
        if 'comment' in str(rule):
            has_comments = True
            print(f"  Found: {rule.rule}")
    
    if not has_comments:
        print("✗ WARNING: No comment routes found!")
        print("  First 10 routes:")
        for i, rule in enumerate(list(api.app.url_map.iter_rules())[:10]):
            print(f"    {rule.rule}")
    
except Exception as e:
    print(f"✗ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYEOF

if [ $? -ne 0 ]; then
    echo "✗ Import test failed! Check error above."
    exit 1
fi

echo ""
echo "=== Step 4: Start Flask ==="
nohup python3 api.py > api.log 2>&1 &
NEW_PID=$!
echo "Started with PID: $NEW_PID"

echo ""
echo "=== Step 5: Wait for startup ==="
sleep 5

echo ""
echo "=== Step 6: Verify process is running ==="
if ps -p $NEW_PID > /dev/null; then
    echo "✓ Process is running (PID: $NEW_PID)"
else
    echo "✗ Process died! Check api.log:"
    tail -n 20 api.log
    exit 1
fi

echo ""
echo "=== Step 7: Test endpoints ==="

# Test 1: Root
echo -n "Testing http://localhost:5000/ ... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5000/)
if [ "$HTTP_CODE" = "404" ] || [ "$HTTP_CODE" = "200" ]; then
    echo "✓ Flask responding (code: $HTTP_CODE)"
else
    echo "✗ No response (code: $HTTP_CODE)"
fi

# Test 2: Existing endpoint
echo -n "Testing /api/exchanges/list ... "
RESULT=$(curl -s http://localhost:5000/api/exchanges/list | grep -o '"success"' | head -n 1)
if [ "$RESULT" = '"success"' ]; then
    echo "✓ Working"
else
    echo "✗ Not working"
fi

# Test 3: Comment endpoint
echo -n "Testing /api/comments/pending ... "
RESULT=$(curl -s http://localhost:5000/api/comments/pending)
if echo "$RESULT" | grep -q '"success"'; then
    echo "✓ Working!"
    echo "  Response: $RESULT"
else
    echo "✗ FAILED"
    echo "  Response: $RESULT"
    echo ""
    echo "Checking api.log for errors:"
    tail -n 30 api.log
fi

echo ""
echo "=== Complete ==="
echo "If comment endpoint still shows 404, run:"
echo "  tail -f ~/projects/mystoreofvalue.com/api.log"
echo "Then try accessing the endpoint and watch for errors"
