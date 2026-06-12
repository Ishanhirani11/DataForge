from dataforge.pipeline import run_pipeline
import tempfile
import os
import streamlit as st
import pandas as pd
import yaml
from dataforge.recommendation import RecommendationEngine
from dataforge.observation import ObservationEngine

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="DataForge",
    page_icon="⚙️",
    layout="wide"
)

st.title("⚙️ DataForge")
st.markdown("### Intelligent Data Preparation Platform")

st.divider()

# ---------------------------------------------------
# UPLOAD DATASET
# ---------------------------------------------------

uploaded_file = st.file_uploader(
    "Upload Dataset",
    type=["csv", "xlsx", "xls", "json"]
)

if uploaded_file:

    # -----------------------------------------
    # LOAD FILE
    # -----------------------------------------
    import os

    os.makedirs("data/uploads", exist_ok=True)

    file_path = f"data/uploads/{uploaded_file.name}"

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
        

    if uploaded_file.name.endswith(".csv"):

        df = pd.read_csv(uploaded_file)

    elif uploaded_file.name.endswith((".xlsx", ".xls")):
    
        df = pd.read_excel(uploaded_file)
    
    elif uploaded_file.name.endswith(".json"):
    
        df = pd.read_json(uploaded_file)
    
    else:
    
        st.error("Unsupported file format")

        st.stop()

    st.success("Dataset uploaded successfully!")

    # -----------------------------------------
    # DATASET SUMMARY
    # -----------------------------------------

    st.divider()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Rows",
            df.shape[0]
        )

    with col2:
        st.metric(
            "Columns",
            df.shape[1]
        )

    with col3:
        st.metric(
            "Missing Values",
            int(df.isnull().sum().sum())
        )

    # -----------------------------------------
    # PREVIEW
    # -----------------------------------------

    st.divider()

    st.subheader("Dataset Preview")

    st.dataframe(
        df.head(),
        use_container_width=True
    )
 
    # -----------------------------------------
    # PROFILE
    # -----------------------------------------

    st.divider()

    st.subheader("Column Profile")

    profile_df = pd.DataFrame({
        "Column": df.columns,
        "Datatype": df.dtypes.astype(str),
        "Missing Count": df.isnull().sum(),
        "Missing %": round(df.isnull().mean()*100,2),
        "Unique Values": df.nunique()
    })

    st.dataframe(
        profile_df,
        use_container_width=True
    )
     
    # -----------------------------------------
    # RECOMMENDATION ENGINE
    # -----------------------------------------

    st.divider()

    st.subheader("Recommendations")

    engine = RecommendationEngine()

    recommendation_df = engine.recommend(df)

    st.dataframe(
        recommendation_df,
        use_container_width=True
    )
    
    # -----------------------------------------
    # AI OBSERVATIONS
    # -----------------------------------------

    st.divider()

    st.subheader("AI Observations")

    observer = ObservationEngine()

    observation_df = observer.observe(df)

    st.dataframe(
        observation_df,
        use_container_width=True
    )

    # -----------------------------------------
    # COLUMN SELECTION
    # -----------------------------------------

    st.divider()

    st.subheader("Select Columns To Keep")

    selected_columns = st.multiselect(
        "Columns",
        options=df.columns,
        default=list(df.columns)
    )

    # -----------------------------------------
    # MISSING VALUE STRATEGIES
    # -----------------------------------------

    st.divider()

    st.subheader("Missing Value Handling")

    strategies = {}

    for col in selected_columns:

        if df[col].isnull().sum() > 0:

            strategies[col] = st.selectbox(
                f"{col} ({round(df[col].isnull().mean()*100,2)}% missing)",
                [
                    "keep_null",
                    "fill_unknown",
                    "fill_mode",
                    "drop_rows"
                ]
            )

    # -----------------------------------------
    # REQUIRED COLUMNS
    # -----------------------------------------

    st.divider()

    st.subheader("Validation")

    required_columns = st.multiselect(
        "Required Columns",
        options=selected_columns
    )

    # -----------------------------------------
    # OUTPUT TYPE
    # -----------------------------------------

    st.divider()

    st.subheader("Output Settings")

    output_type = st.radio(
        "Output Type",
        [
            "csv",
            "json",
            "xlsx",
            "xls"
        ]
    )

    # -----------------------------------------
    # GENERATE YAML
    # -----------------------------------------

    st.divider()

    st.subheader("Generated YAML")

    # -----------------------------------------
    # DETECT SOURCE FORMAT
    # -----------------------------------------
    
    source_format = uploaded_file.name.split(".")[-1].lower()
    
    if source_format in ["xlsx", "xls"]:
        source_format = "excel"
    
    config = {

        "source": {
    
        "type": "file",
    
        "path": file_path,
    
        "format": source_format
    
    },

        "transform": {

            "clean": {

                "missing_value_strategy": "fill_default",

                "missing_value_default": "Unknown",

                "remove_duplicates": True

            },

            "validate": {

                "required": required_columns

            }

        },

        "target": {

            "type": "file",

            "path": f"data/output/cleaned_data.{output_type}",

            "format": output_type

        }

    }

    yaml_text = yaml.dump(
        config,
        sort_keys=False
    )

    st.code(
        yaml_text,
        language="yaml"
    )

    # -----------------------------------------
    # DOWNLOAD YAML
    # -----------------------------------------

    st.download_button(

        label="Download YAML",

        data=yaml_text,

        file_name="pipeline.yaml",

        mime="text/yaml"

    )

    # -----------------------------------------
    # RUN PIPELINE
    # -----------------------------------------

    if st.button("Run Pipeline"):

        # create temp yaml
        with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".yaml",
                delete=False
        ) as f:

            yaml.dump(config, f, sort_keys=False)
            config_path = f.name

        # run pipeline
            
        try:

            result = run_pipeline(config_path)

        except Exception as e:

            st.error(
                f"Pipeline failed: {str(e)}"
            )

            st.stop()

        st.success("Pipeline executed successfully!")

        # show metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Source Rows", result["source_rows"])

        with col2:
            st.metric("Clean Rows", result["clean_rows"])

        with col3:
            st.metric("Valid Rows", result["valid_rows"])

        with col4:
            st.metric("Invalid Rows", result["invalid_rows"])

        # ------------------------------------------------
        # Display cleaned dataset
        # ------------------------------------------------

        output_path = result["output_path"]

        if output_path:

            st.subheader("Cleaned Dataset")

            if output_path.endswith(".csv"):

                output_df = pd.read_csv(output_path)
            
            elif output_path.endswith(".json"):
            
                output_df = pd.read_json(output_path)
            
            elif output_path.endswith((".xlsx", ".xls")):
            
                output_df = pd.read_excel(output_path)
            
            else:
            
                output_df = pd.read_csv(output_path)

            st.dataframe(
                output_df.head(),
                use_container_width=True
            )

            # --------------------------------------------
            # ADD STEP 4 HERE
            # --------------------------------------------

            with open(output_path, "rb") as f:

                st.download_button(

                    label="Download Cleaned Data",

                    data=f,

                    file_name=os.path.basename(output_path),

                    mime="text/csv"

                )
