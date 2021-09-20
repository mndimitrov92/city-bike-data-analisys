"""
Module used to create plots and charts based on the merged data.
Currently the visualizations are done from the exported file of the merged data.
"""
import matplotlib.pyplot as plt
import pandas as pd
plt.rcParams["figure.figsize"] = (10, 5)

DATA = pd.read_csv("output.csv")


def show_gender_ratio():
    """
    Create a pie chart showing the gender ratio of bike riders.
    0 = Unknown
    1 = Male
    2 = Female
    """
    labels = "Male", "Female", "Unknown"
    _, ax = plt.subplots()
    ax.set_title("Bike riders Gender ratio", loc="left")
    ax.pie(DATA["gender"].value_counts(), labels=labels, autopct='%1.1f%%',
           shadow=True, startangle=90)
    ax.axis('equal')
    plt.tight_layout()
    plt.show()


def show_trips_per_usertype():
    """
    Shows a table on the amount of trips based on the user type and the weather conditions.
    """
    cust = list(DATA[DATA["usertype"] == 'Customer']
                ['Precip Type'].value_counts())
    sub = list(DATA[DATA["usertype"] == 'Subscriber']
               ['Precip Type'].value_counts())
    col_labels = ("Trip during rain", "Trip during snow")

    _, ax = plt.subplots()
    ax.table(cellText=[sub, cust], rowLabels=DATA["usertype"].unique(),
             colLabels=col_labels, loc="center")
    ax.set_title(
        "Amount of trips for each user type split by the weather conditions")
    ax.axis('tight')
    ax.axis('off')
    plt.show()


def show_customers_per_day():
    """
    Shows the amount of customers per day.
    """
    all_customers = DATA[DATA["usertype"] == "Customer"]
    days = (all_customers["starttime"].apply(
        lambda x: pd.Timestamp(x).strftime('%d')))
    buckets = days.nunique()
    plt.hist(days, buckets, density=False, facecolor='g', alpha=0.75)
    plt.xlabel('Day of February')
    plt.ylabel('Amount of customers')
    plt.title('Customers per day of month')
    plt.show()


if __name__ == "__main__":
    show_gender_ratio()
    show_trips_per_usertype()
    show_customers_per_day()
