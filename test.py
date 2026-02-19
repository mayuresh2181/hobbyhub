import datetime

print("Hello Mayuresh 🚀")
print("GitHub Actions is working!")

now = datetime.datetime.now()
print("Current time:", now)

with open("output_testpy.txt", "w") as f:
    f.write("GitHub Actions test successful!\n")
    f.write("Time: " + str(now))

print("File created successfully.")
