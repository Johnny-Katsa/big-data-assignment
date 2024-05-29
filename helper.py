import pyfiglet

NUM_HASHTAGS = 50


def intro_print(text):
    print(["#"] * NUM_HASHTAGS)
    print(pyfiglet.figlet_format(text))
    print(["#"] * NUM_HASHTAGS)


def outro_print():
    print(["#"] * NUM_HASHTAGS)
    print(pyfiglet.figlet_format("Execution complete!"))
    print(["#"] * NUM_HASHTAGS)
