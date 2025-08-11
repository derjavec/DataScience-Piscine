from give_bmi import give_bmi, apply_limit


def test_bmi(height, weight, limit):
    print(f"Testing with height={height} and weight={weight}")
    try:
        bmi = give_bmi(height, weight)
        print("BMI:", bmi)
    except ValueError as e:
        print("Error in give_bmi:", e)
        return

    try:
        result = apply_limit(bmi, limit)
        print("apply_limit result:", result)
    except ValueError as e:
        print("Error in apply_limit:", e)


print("=== TEST CASE 1 ===")
test_bmi([2.71, 1.15], [165.3, 38.4], 26)

print("\n=== TEST CASE 2: Mismatched lengths ===")
test_bmi([1.8], [70, 80], 26)

print("\n=== TEST CASE 3: Invalid type in height ===")
test_bmi([1.8, "bad"], [70, 80], 26)

print("\n=== TEST CASE 4: Invalid type in weight ===")
test_bmi([1.8, 1.7], [70, None], 26)

print("\n=== TEST CASE 5: Invalid BMI type ===")
try:
    print(apply_limit([23.5, "bad"], 26))
except ValueError as e:
    print("Error in apply_limit:", e)
