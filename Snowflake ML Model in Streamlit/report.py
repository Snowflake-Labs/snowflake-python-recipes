from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import Registry
from snowflake.ml._internal.utils import identifier

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt


session = get_active_session()

# Get the model from registry
db = identifier._get_unescaped_name(session.get_current_database())
schema = identifier._get_unescaped_name(session.get_current_schema())
model_name = "DIAMONDS_PRICE_PREDICTION"
native_registry = Registry(session=session, database_name=db, schema_name=schema)
model = native_registry.get_model(model_name).version('V3')

st.title("Diamond Price Predictor")

# Create input form
with st.form("diamond_predictor"):
    col1, col2, col3 = st.columns(3)
    
    with col1:
        cut = st.selectbox("Cut", ["Fair", "Good", "Very Good", "Premium", "Ideal"])
        carat = st.number_input("Carat", min_value=0.2, max_value=5.0, value=1.0, step=0.1)
        depth = st.number_input("Depth", min_value=43.0, max_value=79.0, value=61.5, step=0.1)
        
    with col2:
        color = st.selectbox("Color", ['D', 'E', 'F', 'G', 'H', 'I', 'J'])
        x_dim = st.number_input("Length (X)", min_value=0.0, max_value=10.0, value=5.0, step=0.1)
        y_dim = st.number_input("Width (Y)", min_value=0.0, max_value=10.0, value=5.0, step=0.1)
        
    with col3:
        clarity = st.selectbox("Clarity", ["IF", "VVS1", "VVS2", "VS1", "VS2", "SI1", "SI2", "I1"])
        z_dim = st.number_input("Depth (Z)", min_value=0.0, max_value=10.0, value=3.0, step=0.1)
        table = st.number_input("Table", min_value=43.0, max_value=95.0, value=57.0, step=0.1)
    
    submitted = st.form_submit_button("Predict Price")

cut_encoding = {
    'Fair': 0,
    'Good': 1,
    'Very Good': 2,
    'Premium': 3,
    'Ideal': 4
}

color_encoding = {
    'D': 0,
    'E': 1,
    'F': 2,
    'G': 3,
    'H': 4,
    'I': 5,
    'J': 6
}
clarity_encoding = {
    'I1': 0,
    'SI2': 1,
    'SI1': 2,
    'VS2': 3,
    'VS1': 4,
    'VVS2': 5,
    'VVS1': 6,
    'IF': 7
}

if submitted:
    try:
        # Create a DataFrame with the input values
        input_data = pd.DataFrame({
            'CUT_OE': [cut_encoding[cut]],
            'COLOR_OE': [color_encoding[color]],
            'CLARITY_OE': [clarity_encoding[clarity]],
            'CARAT': [carat],
            'DEPTH': [depth],
            'TAB': [table], 
            'X': [x_dim],
            'Y': [y_dim],
            'Z': [z_dim]
        })
        
        # Make prediction directly using the model
        prediction = model.run(input_data, function_name="predict")
        predicted_price = prediction['output_feature_0'].iloc[0]
        
        # Display result
        st.success(f"Predicted Diamond Price: ${predicted_price:,.2f}")

         # 1. Feature Importance Chart
        feature_values = pd.DataFrame({
            'Feature': ['Cut', 'Color', 'Clarity', 'Carat', 'Depth', 'Table', 'Length', 'Width', 'Depth (Z)'],
            'Value': [cut_encoding[cut], color_encoding[color], clarity_encoding[clarity], 
                     carat, depth, table, x_dim, y_dim, z_dim]
        })
        
        feature_chart = alt.Chart(feature_values).mark_bar().encode(
            x=alt.X('Value:Q'),
            y=alt.Y('Feature:N', sort='-x'),
            color=alt.Color('Value:Q', scale=alt.Scale(scheme='viridis'))
        ).properties(
            title='Current Diamond Characteristics',
            width=600,
            height=300
        )
        
        st.altair_chart(feature_chart)
        
        # 2. Diamond Dimensions and Quality Metrics side by side
        col1, col2 = st.columns(2)
        
        with col1:
            dimensions_data = pd.DataFrame({
                'Dimension': ['Length (X)', 'Width (Y)', 'Depth (Z)'],
                'Value': [x_dim, y_dim, z_dim]
            })
            
            dim_chart = alt.Chart(dimensions_data).mark_circle(size=200).encode(
                x=alt.X('Value:Q', scale=alt.Scale(domain=[0, 10])),
                y=alt.Y('Dimension:N'),
                size='Value:Q',
                color=alt.Color('Value:Q', scale=alt.Scale(scheme='blues'))
            ).properties(
                title='Diamond Dimensions',
                width=300,
                height=200
            )
            
            st.altair_chart(dim_chart)
            
        with col2:
            quality_metrics = pd.DataFrame({
                'Metric': ['Cut', 'Color', 'Clarity'],
                'Score': [
                    cut_encoding[cut]/4 * 100,
                    (6 - color_encoding[color])/6 * 100,
                    clarity_encoding[clarity]/7 * 100
                ]
            })
            
            quality_chart = alt.Chart(quality_metrics).mark_bar().encode(
                x=alt.X('Metric:N', title=None),
                y=alt.Y('Score:Q', scale=alt.Scale(domain=[0, 100]), title='Quality Score (%)'),
                color=alt.Color('Metric:N', scale=alt.Scale(scheme='category10')),
                tooltip=['Metric', 'Score']
            ).properties(
                width=300,
                height=200,
                title={
                    'text': 'Diamond Quality Metrics',
                    'offset': 10,
                    'fontSize': 14,
                    'anchor': 'start'
                }
            )
            
            text = quality_chart.mark_text(
                align='center',
                baseline='bottom',
                dy=-5
            ).encode(
                text=alt.Text('Score:Q', format='.1f')
            )
    
            final_quality_chart = (quality_chart + text)
            st.altair_chart(final_quality_chart, use_container_width=True)

            
    except Exception as e:
        st.error(f"Error making prediction: {str(e)}")

