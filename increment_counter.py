import streamlit as st

def increment(session_state_attribute,session_state_attribute_list):
    st.session_state[session_state_attribute] += 1
    st.session_state[session_state_attribute_list].append(st.session_state[session_state_attribute])