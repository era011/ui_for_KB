import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from  backend.weaviate_funcs import *
import streamlit as st
import time
import io
import pandas as pd
from backend.db import *

css_path = os.path.join(
    os.path.dirname(__file__),
    "style",
    "style.css"
)


init_pg_pool()
# ensure_marketing_files_table()


title=None
if "chunks" not in st.session_state:
    st.session_state.chunks = ""


client = get_client()
with open(css_path, encoding="utf-8") as f:
    st.html(f"<style>{f.read()}</style>")

st.set_page_config(page_title="Альфа Ойл", layout="wide")


def correct_name_doc(name: str) -> str:
    if len(name) <= 140:
        return name
    else:
        return name[:140-6] + " . . ."
    
def delete_document(client, id_doc: str):
    delete_document_postgres(id_doc)
    delete_document_weaviate(client, id_doc)


with st.container(key='block_docs',height=475, border=True):
    docss = search_marketing_files_pg(title=title)
    if docss:
        for d in docss:
            col1, col2 = st.columns([0.965, 0.035])
            with col1:
                if st.button(f'{correct_name_doc(f"📄 {d['name']}")}         {d['chunks_count']}',key=f"docs_{d['id_doc']}"):  
                    st.session_state.chunks = '\n\n--------------------------------------------------------\n\n'.join(get_chunks_weaviate(client, d['id_doc']))
            with col2:
                if st.button("🗑", key=f"del_{d['id_doc']}"):
                    # docss.remove(d)
                    delete_document(client, d['id_doc'])
                    docss = search_marketing_files_pg(title=title)
    else:
        st.warning("Ничего не найдено")

if st.session_state.chunks!='':    
    with st.container(key='block_chunks',height=475, border=True):
        st.write(st.session_state.chunks)
    if st.button("Очистить"):
        st.session_state.chunks=''    


if st.session_state.chunks=='':
    with st.container(vertical_alignment ='bottom',horizontal=True, border=True):
        uploaded_files = st.file_uploader(
        "Выберите PDF файл", accept_multiple_files=True,type='txt'
        )

    if st.button("Добавить в базу",key='add_button',width=200):
        for uploaded_file in uploaded_files:
            try:
                status_placeholder = st.empty()
                status_placeholder.write("Добавляем...")
                docs = add_text_document_to_weaviate(uploaded_file)   
                status_placeholder.success(f"✅ Документ {uploaded_file.name} добавлен в базу")
            except Exception as e:
                status_placeholder.error(f"❌ Ошибка при добавлении в базу файла {uploaded_file.name}: {e}")