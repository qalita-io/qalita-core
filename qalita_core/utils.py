### Contains general utility functions ###
import re
import unicodedata

# Constants for determine_level function
INFO_THRESHOLD = 70
WARNING_THRESHOLD = 90
HIGH_THRESHOLD = 100


def get_version():
    return "0.0.0-dev"


# Function to determine recommendation level based on duplication rate
def determine_recommendation_level(dup_rate):
    if dup_rate > 0.7:
        return "high"
    elif dup_rate > 0.3:
        return "warning"
    else:
        return "info"


def extract_variable_name(content):
    # Regular expression pattern to extract variable name
    pattern = r"^(.*?)\s+(has|is)"
    match = re.search(pattern, content)
    return match.group(1) if match else ""


def round_if_numeric(value, decimals=2):
    try:
        # Convert to a float and round
        rounded_value = round(float(value), decimals)
        # Format it as a string with the specified number of decimal places
        return f"{rounded_value:.{decimals}f}".rstrip("0").rstrip(
            "."
        )  # Removes trailing zeros and dot if it's an integer
    except (ValueError, TypeError):
        # Return the original value if it's not a number
        return str(value)


def determine_level(content):
    match = re.search(r"(\d+(\.\d+)?)%", content)
    if match:
        percentage = float(match.group(1))
        if percentage <= INFO_THRESHOLD:
            return "info"
        elif percentage <= WARNING_THRESHOLD:
            return "warning"
        elif percentage <= HIGH_THRESHOLD:
            return "high"
    return "info"


# Denormalize a dictionary with nested dictionaries
def denormalize(data):
    denormalized = {}
    for index, content in data.items():
        if isinstance(content, dict):
            for inner_key, inner_value in content.items():
                new_key = f"{index}_{inner_key.lower()}"
                denormalized[new_key] = inner_value
        else:
            denormalized[index] = content
    return denormalized


def slugify(column_name):
    # Normalize the unicode string to decompose the accents
    column_name = unicodedata.normalize("NFKD", column_name)

    # Convert to lower case and strip leading/trailing spaces
    column_name = column_name.lower().strip()

    # Encode to ASCII bytes, then decode back to string ignoring non-ASCII characters
    column_name = column_name.encode("ascii", "ignore").decode("ascii")

    # Replace any sequence of non-alphanumeric characters with a single underscore
    column_name = re.sub(r"[\W_]+", "_", column_name)

    return column_name


def replace_whitespaces_with_underscores(df):
    column_name_association = {}
    for col in df.columns:
        slugified_col = slugify(col)
        column_name_association[slugified_col] = col
        df = df.rename(columns={col: slugified_col})
    return df, column_name_association
