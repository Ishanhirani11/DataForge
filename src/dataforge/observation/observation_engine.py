import pandas as pd


IMPORTANT_KEYWORDS = [

    "name",
    "customer",
    "supplier",
    "importer",
    "exporter",
    "email",
    "phone",
    "id"

]


class ObservationEngine:

    def observe(self, df: pd.DataFrame):

        observations = []

        for col in df.columns:

            missing_pct = round(
                df[col].isnull().mean() * 100,
                2
            )

            text = ""

            if missing_pct > 80:

                text = (
                    "Column contains excessive missing values "
                    "and may be removed."
                )

            elif missing_pct > 50:

                if any(
                    keyword in col.lower()
                    for keyword in IMPORTANT_KEYWORDS
                ):

                    text = (
                        "This column appears business critical "
                        "and should be reviewed before removal."
                    )

                else:

                    text = (
                        "Column contains many missing values "
                        "and should be reviewed."
                    )

            else:

                text = (
                    "Column quality is acceptable."
                )

            observations.append(

                {
                    "Column": col,
                    "Observation": text
                }

            )

        return pd.DataFrame(observations)