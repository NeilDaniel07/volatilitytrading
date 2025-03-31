## Installation

```bash
git clone https://github.com/NeilDaniel07/volatilitytrading.git
cd volatilitytrading
pip install -r requirements.txt
```

## Screening Criteria

Each stock must pass **all three** thresholds to appear in the final output:

| Metric              | Description                                               | Default Threshold     |
|---------------------|-----------------------------------------------------------|------------------------|
| Average Volume      | 30-day average trading volume                             | ≥ 1,500,000 shares     |
| IV30 / RV30 Ratio   | Ratio of implied to realized volatility over 30 days      | ≥ 1.25                 |
| Term Structure Slope| Slope of IV curve from near-term to 45-day expiration     | ≤ -0.00406             |

---

## How It Works

1. Provide an earnings date (e.g., `"2025-03-26"`)
2. Script fetches all U.S. stocks with earnings that day
3. It filters for Nasdaq/NYSE tickers
4. It retrieves option chains and calculates volatility metrics
5. Results that pass all filters are returned in a pandas DataFrame

---

## Implementation Details

The screening logic is structured in a class called SimpleEarningsApp. This class's initialization function accepts four parameters. The first is the earnings date formatted as a string.
The remaining three are the average volume threshold, iv30/rv30 ratios, and term structure slope ratio (in this order), formatted as floats. The resultant dataframe of stocks that pass all three
thresholds is accessible through the outputDF instance variable of the class. See example usage below. 

## Example Client Code Utilization

```bash
def main():
    desiredVolumeThreshold = float(input("Desired Avergage Volume Threshold: "))
    desiredIVRVThreshold = float(input("Desired IV30/RV30 Ration Threshold: "))
    desiredTSSThreshold = float(input("Desired Term Slope Threshold: "))
    app = SimpleEarningsApp("2025-03-26", desiredVolumeThreshold, desiredIVRVThreshold, desiredTSSThreshold)
    print(app.outputDF)

main()
```
