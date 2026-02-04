#!/usr/bin/env python3
"""
Unit tests for clean_number function
"""

import sys
import os
sys.path.append('/Users/alex/Projects/food')

from bot import clean_number

def test_clean_number():
    """
    Test clean_number function with various inputs
    """
    test_cases = [
        # (input, expected_output, description)
        ('Ставка НДС: Без НДС\n1 200,00 ₽', 1200.0, 'Multiline with NDS info'),
        ('2 233 843,92\nСтавка НДС: 20%', 2233843.92, 'Multiline with percentage'),
        ('1 200,00 ₽', 1200.0, 'Simple price with ruble'),
        ('0', 0.0, 'Zero value'),
        ('', 0.0, 'Empty string'),
        (None, 0.0, 'None value'),
        ('Ставка НДС: Без НДС\n0', 0.0, 'Multiline with zero only'),
        ('5 000', 5000.0, 'Simple number with space'),
        ('5 000,50', 5000.5, 'Number with decimal comma'),
        ('5 000.50', 5000.5, 'Number with decimal dot'),
        ('ДЕТ ДН\n120', 120.0, 'Units with number'),
        ('УСЛ ЕД\n1 500,00', 1500.0, 'Services with number'),
    ]
    
    passed = 0
    failed = 0
    
    print("🧪 Testing clean_number function:")
    print("=" * 50)
    
    for i, (input_data, expected, description) in enumerate(test_cases, 1):
        try:
            result = clean_number(input_data)
            if abs(result - expected) < 0.01:  # Allow small floating point differences
                print(f"✅ Test {i:02d}: {description}")
                print(f"   Input: {repr(input_data)}")
                print(f"   Expected: {expected}, Got: {result}")
                print()
                passed += 1
            else:
                print(f"❌ Test {i:02d}: {description}")
                print(f"   Input: {repr(input_data)}")
                print(f"   Expected: {expected}, Got: {result}")
                print(f"   Difference: {abs(result - expected)}")
                print()
                failed += 1
        except Exception as e:
            print(f"💥 Test {i:02d}: {description}")
            print(f"   Input: {repr(input_data)}")
            print(f"   Error: {e}")
            print()
            failed += 1
    
    print("=" * 50)
    print(f"📊 Results: {passed} passed, {failed} failed")
    print(f"🎯 Success rate: {(passed/(passed+failed)*100):.1f}%")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return True
    else:
        print("⚠️ Some tests failed!")
        return False

if __name__ == "__main__":
    success = test_clean_number()
    sys.exit(0 if success else 1)