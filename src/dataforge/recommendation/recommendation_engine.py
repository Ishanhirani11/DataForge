import pandas as pd


class RecommendationEngine:

    def recommend(self, df: pd.DataFrame):

        recommendations = []

        for col in df.columns:

            missing_pct = round(
                df[col].isnull().mean() * 100,
                2
            )

            if missing_pct > 80:

                recommendation = "REMOVE"

            elif missing_pct > 50:

                recommendation = "REVIEW"

            else:

                recommendation = "KEEP"

            recommendations.append(
                {
                    "Column": col,
                    "Missing %": missing_pct,
                    "Recommendation": recommendation
                }
            )

        return pd.DataFrame(recommendations)