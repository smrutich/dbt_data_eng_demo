import yaml
import requests
from pathlib import Path
import streamlit as st
from snowflake.core import Root
from snowflake.snowpark import Session
# from snowflake.core.cortex.analyst import Analyst, Message, UserMessage, TextContent
from os import environ
from dotenv import load_dotenv
load_dotenv()


MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]
SEMANTIC_MODEL_FILE = "@DBT_TEST_ANNA.PUBLIC.SEMANTIC_MODELS/semantic_model.yml"
icons = {"assistant": "❄️", "user": "👤"}

def get_session():
    """
    Creates and returns a Snowflake session using credentials from profiles.yml and environment variables.
    
    This function reads the dbt profiles.yml configuration file to extract Snowflake connection
    parameters (account, user, role, warehouse, database, schema) and combines them with the
    password from the SNOWFLAKE_PASSWORD environment variable to establish a Snowflake session.
    
    Returns:
        Session: A configured Snowflake Snowpark session object ready for executing queries.
        
    Raises:
        FileNotFoundError: If profiles.yml is not found.
        KeyError: If required configuration keys are missing or SNOWFLAKE_PASSWORD env var is not set.
    """
    with open("profiles.yml".replace("~", str(Path.home()))) as f:
        profiles = yaml.safe_load(f)

    dbt_config = profiles["test"]["outputs"]["dev"]
    connection_params = {
        "account": dbt_config["account"],
        "user": dbt_config["user"],
        "password": environ["SNOWFLAKE_PASSWORD"],
        "role": dbt_config["role"],
        "warehouse": dbt_config["warehouse"],
        "database": dbt_config["database"],
        "schema": dbt_config["schema"],
    }
    return Session.builder.configs(connection_params).create()

def init_messages():
    """
    Initializes or clears the chat message history in Streamlit session state.
    
    This function checks if the conversation should be cleared (via the clear_conversation button)
    or if the messages list doesn't exist yet. In either case, it initializes/resets the messages
    list to an empty array.
    
    Side Effects:
        Modifies st.session_state.messages to be an empty list.
    """
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def init_service_metadata():
    """
    Retrieves and caches metadata about available Snowflake Cortex Search Services.
    
    This function queries Snowflake to get all available Cortex Search Services, then for each
    service, it retrieves the search column name. The metadata is stored in session state to
    avoid repeated queries. Each service metadata includes the service name and its search column.
    
    Side Effects:
        - Executes SQL queries against Snowflake (SHOW CORTEX SEARCH SERVICES and DESC commands)
        - Stores service_metadata list in st.session_state with structure:
          [{"name": str, "search_column": str}, ...]
    
    Note:
        Only runs if service_metadata is not already in session state (caching behavior).
    """
    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s["name"]
                svc_search_col = session.sql(
                    f"DESC CORTEX SEARCH SERVICE {svc_name};"
                ).collect()[0]["search_column"]
                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col}
                )
        st.session_state.service_metadata = service_metadata

def init_config_options():
    """
    Creates and displays all configuration UI elements in the Streamlit sidebar.
    
    This function sets up the sidebar with various controls:
    - Dropdown to select which Cortex Search Service to use
    - Button to clear the conversation history
    - Toggle for debug mode (shows context documents and chat history summaries)
    - Toggle to enable/disable chat history usage
    - Advanced options expander containing:
      * Model selection (mistral-large, snowflake-arctic, llama3-70b, llama3-8b)
      * Number of context chunks to retrieve (1-10, default 5)
      * Number of chat messages to include in history (1-10, default 5)
    - Session state viewer for debugging
    
    Side Effects:
        Creates UI elements in the Streamlit sidebar and stores user selections in session state
        with keys: selected_cortex_search_service, clear_conversation, debug, use_chat_history,
        model_name, num_retrieved_chunks, num_chat_messages.
    """
    st.sidebar.selectbox(
        "Select cortex search service:",
        [s["name"] for s in st.session_state.service_metadata],
        key="selected_cortex_search_service",
    )
    st.sidebar.button("Clear conversation", key="clear_conversation")
    st.sidebar.toggle("Debug", key="debug", value=False)
    st.sidebar.toggle("Use chat history", key="use_chat_history", value=True)
    with st.sidebar.expander("Advanced options"):
        st.selectbox("Select model:", MODELS, key="model_name")
        st.number_input("Select number of context chunks", value=5, key="num_retrieved_chunks", min_value=1, max_value=10)
        st.number_input("Select number of messages to use in chat history", value=5, key="num_chat_messages", min_value=1, max_value=10)
    st.sidebar.expander("Session State").write(st.session_state)

def query_cortex_search_service(query):
    """
    Queries the selected Snowflake Cortex Search Service and retrieves relevant context documents.
    
    This function performs a semantic search using the selected Cortex Search Service to find
    documents relevant to the user's query. It retrieves the top N documents (based on
    num_retrieved_chunks setting) and formats them as a concatenated string. If debug mode
    is enabled, the context documents are displayed in the sidebar.
    
    Args:
        query (str): The search query to find relevant documents.
        
    Returns:
        str: A formatted string containing all retrieved context documents, numbered and
             separated by newlines. Format: "Context document 1: <content>\n\nContext document 2: ..."
    
    Side Effects:
        - Executes a search query against the Snowflake Cortex Search Service
        - If debug mode is on, displays context documents in the sidebar
    """
    db, schema = session.get_current_database(), session.get_current_schema()
    cortex_search_service = (
        root.databases[db]
        .schemas[schema]
        .cortex_search_services[st.session_state.selected_cortex_search_service]
    )
    context_documents = cortex_search_service.search(query, columns=[], limit=st.session_state.num_retrieved_chunks)
    results = context_documents.results
    service_metadata = st.session_state.service_metadata
    search_col = [s["search_column"] for s in service_metadata if s["name"] == st.session_state.selected_cortex_search_service][0]
    context_str = ""
    for i, r in enumerate(results):
        context_str += f"Context document {i+1}: {r[search_col]} \n\n"
    if st.session_state.debug:
        st.sidebar.text_area("Context documents", context_str, height=500)
    return context_str

def get_chat_history():
    """
    Retrieves the most recent chat messages to use as conversation context.
    
    This function extracts the last N messages from the conversation history (where N is
    determined by num_chat_messages setting), excluding the most recent message (which is
    typically the current user question being processed).
    
    Returns:
        list: A list of message dictionaries, each containing 'role' and 'content' keys.
              Returns up to num_chat_messages recent messages, excluding the last one.
              Returns empty list if there are no previous messages.
    
    Example:
        If messages = [msg1, msg2, msg3, msg4, msg5] and num_chat_messages = 3,
        returns [msg3, msg4] (last 3 minus the most recent one).
    """
    start_index = max(0, len(st.session_state.messages) - st.session_state.num_chat_messages)
    return st.session_state.messages[start_index : len(st.session_state.messages) - 1]

def complete(model, prompt):
    """
    Calls Snowflake Cortex Complete API to generate AI responses using the specified model.
    
    This is a wrapper function that executes the Snowflake Cortex Complete function via SQL,
    which generates text completions using large language models hosted in Snowflake.
    
    Args:
        model (str): The name of the LLM model to use (e.g., "mistral-large", "llama3-70b").
        prompt (str): The prompt text to send to the model for completion.
        
    Returns:
        str: The generated text response from the AI model.
        
    Note:
        Uses parameterized SQL query to safely pass model and prompt values.
    """
    return session.sql("SELECT snowflake.cortex.complete(?,?)", (model, prompt)).collect()[0][0]

def make_chat_history_summary(chat_history, question):
    """
    Generates a contextualized query by combining chat history with the current question.
    
    This function uses an LLM to create an enhanced version of the user's question that
    incorporates relevant context from the conversation history. This helps the search
    service find more relevant documents by understanding the full conversational context.
    
    Args:
        chat_history (list): List of previous message dictionaries from the conversation.
        question (str): The current user question.
        
    Returns:
        str: An enhanced natural language query that extends the question with chat history context.
        
    Side Effects:
        - Calls the Snowflake Cortex Complete API
        - If debug mode is on, displays the generated summary in the sidebar
        
    Example:
        If chat history discusses "pizza sales" and question is "What about last month?",
        might return "What were the pizza sales last month?"
    """
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.
        <chat_history>{chat_history}</chat_history>
        <question>{question}</question>
        [/INST]
    """
    summary = complete(st.session_state.model_name, prompt)
    if st.session_state.debug:
        st.sidebar.text_area("Chat history summary", summary.replace("$", "\$"), height=150)
    return summary

def create_prompt(user_question):
    """
    Creates a comprehensive RAG (Retrieval-Augmented Generation) prompt for the AI assistant.
    
    This function orchestrates the RAG pipeline by:
    1. Optionally incorporating chat history to contextualize the question
    2. Retrieving relevant documents from the Cortex Search Service
    3. Constructing a detailed prompt that includes the question, context, and chat history
    
    The prompt instructs the AI to act as a helpful assistant that uses the provided context
    and chat history to answer questions accurately and concisely.
    
    Args:
        user_question (str): The user's current question.
        
    Returns:
        str: A formatted prompt string ready to be sent to the LLM, containing:
             - System instructions for the AI assistant
             - Chat history (if enabled and available)
             - Retrieved context documents
             - The user's question
             
    Behavior:
        - If use_chat_history is enabled and history exists: Creates a contextualized query
          using chat history, then retrieves relevant documents
        - If use_chat_history is disabled or no history exists: Directly retrieves documents
          using the original question
        - Always includes instructions for the AI to respond appropriately and avoid
          phrases like "according to the provided context"
    """
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history != []:
            question_summary = make_chat_history_summary(chat_history, user_question)
            prompt_context = query_cortex_search_service(question_summary)
        else:
            prompt_context = query_cortex_search_service(user_question)
    else:
        prompt_context = query_cortex_search_service(user_question)
        chat_history = ""
    prompt = f"""
        [INST]
        You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
        you will also be given context provided between <context> and </context> tags. Use that context
        with the user's chat history provided in the between <chat_history> and </chat_history> tags
        to provide a summary that addresses the user's question. Ensure the answer is coherent, concise,
        and directly relevant to the user's question.
        If the user asks a generic question which cannot be answered with the given context or chat_history,
        just say "I don't know the answer to that question."
        Don't saying things like "according to the provided context".
        <chat_history>{chat_history}</chat_history>
        <context>{prompt_context}</context>
        <question>{user_question}</question>
        [/INST]
        Answer:
    """
    return prompt

def send_analyst_message(prompt: str) -> dict:
    """
    Sends a user prompt to Snowflake Cortex Analyst API and returns the response.
    
    This function makes a REST API call to Snowflake's Cortex Analyst service to analyze
    natural language questions about structured data. The Analyst uses a semantic model
    to understand the data schema and generate appropriate SQL queries with explanations.
    
    Args:
        prompt (str): The user's natural language question about the data.
        
    Returns:
        dict: Response dictionary containing:
            - message: Dict with 'content' list of response blocks (text, sql, suggestions)
            - request_id: Snowflake request ID for debugging/tracking
            
    Raises:
        Exception: If the API returns a status code >= 400, includes request ID and error details.
        
    Note:
        - Uses the global SEMANTIC_MODEL_FILE constant for the semantic model location
        - Authenticates using the session's REST token
        - Account identifier is hardcoded as "jsa18243" (should be made configurable)
        - Creates a new message with role "user" for each prompt (no conversation history)
    """
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_model_file": SEMANTIC_MODEL_FILE,
    }
    resp = requests.post(
        url=f"https://jsa18243.snowflakecomputing.com/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{session.connection._rest.token}"',
            "Content-Type": "application/json",
        },
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": request_id}
    else:
        raise Exception(f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}")

def display_analyst_content(content, request_id=None, message_index=None):
    """
    Renders Cortex Analyst response content in the Streamlit UI with appropriate formatting.
    
    This function displays different types of content blocks returned by the Cortex Analyst:
    - Text: Natural language explanations rendered as markdown
    - SQL: Generated queries shown in code blocks with executable results
    - Suggestions: Follow-up questions as clickable buttons
    
    For SQL queries with multiple rows, creates tabs for Data, Line Chart, and Bar Chart views.
    
    Args:
        content (list): List of content block dictionaries from Analyst response.
                       Each block has a 'type' field and type-specific data.
        request_id (str, optional): Snowflake request ID for debugging. Shown in expander if provided.
        message_index (int, optional): Index of the message in conversation history.
                                      Used for unique button keys. Defaults to current message count.
    
    Side Effects:
        - Renders markdown, code blocks, dataframes, and charts in Streamlit
        - Executes SQL queries against Snowflake
        - Creates interactive buttons for suggestions that set active_analyst_suggestion in session state
        - May trigger re-run when suggestion button is clicked
        
    Content Block Types:
        - "text": {"type": "text", "text": str}
        - "sql": {"type": "sql", "statement": str}
        - "suggestions": {"type": "suggestions", "suggestions": [str, ...]}
    """
    message_index = message_index or len(st.session_state.analyst_messages)
    if request_id:
        with st.expander("Request ID", expanded=False):
            st.markdown(request_id)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"analyst_{message_index}_{suggestion_index}"):
                        st.session_state.active_analyst_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    df = session.sql(item["statement"]).to_pandas()
                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab = st.tabs(["Data", "Line Chart", "Bar Chart"])
                        data_tab.dataframe(df)
                        if len(df.columns) > 1:
                            df = df.set_index(df.columns[0])
                        with line_tab:
                            st.line_chart(df)
                        with bar_tab:
                            st.bar_chart(df)
                    else:
                        st.dataframe(df)

def process_analyst_message(prompt: str):
    """
    Processes a user prompt through the Cortex Analyst pipeline and displays the response.
    
    This function orchestrates the complete flow for handling a user question:
    1. Adds the user message to conversation history
    2. Displays the user's question in the chat interface
    3. Sends the prompt to Cortex Analyst API
    4. Displays the analyst's response (text, SQL, suggestions)
    5. Adds the assistant's response to conversation history
    
    Args:
        prompt (str): The user's natural language question about the data.
        
    Side Effects:
        - Appends user and assistant messages to st.session_state.analyst_messages
        - Renders chat messages in the Streamlit UI
        - Calls send_analyst_message() to interact with Cortex Analyst API
        - Calls display_analyst_content() to render the response
        - Shows a spinner while waiting for the API response
        
    Message Structure:
        User message: {"role": "user", "content": [{"type": "text", "text": str}]}
        Assistant message: {"role": "assistant", "content": [...], "request_id": str}
    """
    st.session_state.analyst_messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user", avatar=icons["user"]):
        st.markdown(prompt)
    with st.chat_message("assistant", avatar=icons["assistant"]):
        with st.spinner("Generating response..."):
            response = send_analyst_message(prompt=prompt)
            request_id = response["request_id"]
            content = response["message"]["content"]
            display_analyst_content(content=content, request_id=request_id)
    st.session_state.analyst_messages.append(
        {"role": "assistant", "content": content, "request_id": request_id}
    )

def run_analyst_tab():
    """
    Renders and manages the Cortex Analyst tab for natural language queries on structured data.
    
    This function creates an interactive chat interface where users can ask questions about
    pizza data in natural language. The Cortex Analyst interprets questions, generates SQL
    queries based on a semantic model, executes them, and presents results with visualizations.
    
    Features:
        - Displays conversation history with all previous questions and responses
        - Shows the semantic model file being used
        - Handles user input through chat interface
        - Supports clickable suggestion buttons that auto-submit follow-up questions
        - Maintains separate message history from the RAG chatbot (analyst_messages)
        
    Session State Variables:
        - analyst_messages: List of conversation messages (user and assistant)
        - active_analyst_suggestion: Temporarily stores clicked suggestion for processing
        
    Flow:
        1. Initialize session state if needed
        2. Display all previous messages with their content
        3. Accept new user input via chat input
        4. Process suggestions clicked in previous responses
        5. Clear active suggestion after processing
        
    Side Effects:
        - Initializes st.session_state.analyst_messages and active_analyst_suggestion
        - Renders UI elements (subheader, markdown, chat messages)
        - Calls process_analyst_message() for new inputs and suggestions
        - Modifies session state when suggestions are clicked
        
    Note:
        - Uses SEMANTIC_MODEL_FILE constant (defined at module level)
        - Suggestion buttons trigger a re-run to process the suggested question
        - Each message is displayed using display_analyst_content() for proper formatting
    """
    st.subheader("Ask questions about your structured pizza data")
    st.markdown("Semantic Model: `semantic_model.yml`")

    if "analyst_messages" not in st.session_state:
        st.session_state.analyst_messages = []
        st.session_state.active_analyst_suggestion = None

    for message_index, message in enumerate(st.session_state.analyst_messages):
        role = "assistant" if message["role"] != "user" else "user"
        with st.chat_message(role, avatar=icons[role]):
            display_analyst_content(
                content=message["content"],
                request_id=message.get("request_id"),
                message_index=message_index,
            )

    if user_input := st.chat_input("Ask about your pizza data...", key="analyst_input"):
        process_analyst_message(prompt=user_input)

    if st.session_state.get("active_analyst_suggestion"):
        process_analyst_message(prompt=st.session_state.active_analyst_suggestion)
        st.session_state.active_analyst_suggestion = None

def send_agent_message(messages):
    request_body = {
        "messages": messages,
        "stream": False,
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "pizza_analyst",
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "pizza_search",
                }
            },
        ],
        "tool_resources": {
            "pizza_analyst": {
                "semantic_model_file": SEMANTIC_MODEL_FILE,
                "execution_environment": {
                    "type": "warehouse",
                    "warehouse": "ANNA_TEST_WH",
                },
            },
            "pizza_search": {
                "search_service": "DBT_TEST_ANNA.PUBLIC.PIZZA_PDF_CHUNKS",
            },
        },
    }
    resp = requests.post(
        url="https://jsa18243.snowflakecomputing.com/api/v2/cortex/agent:run",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{session.connection._rest.token}"',
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout=60,
    )
    if resp.status_code < 400:
        return resp.json()
    else:
        raise Exception(f"Agent request failed with status {resp.status_code}: {resp.text}")

def display_agent_content(content):
    icons = {"assistant": "❄️", "user": "👤"}
    for item in content:
        if item["type"] == "text":
            st.markdown(item.get("text", ""))
        elif item["type"] == "tool_result":
            tool_result = item.get("tool_result", {})
            tool_type = tool_result.get("type", "")
            for result_content in tool_result.get("content", []):
                if result_content.get("type") == "json":
                    json_data = result_content.get("json", {})
                    if "sql" in json_data:
                        with st.expander("SQL Query", expanded=False):
                            st.code(json_data["sql"], language="sql")
                    if "result_set" in json_data:
                        result_set = json_data["result_set"]
                        columns = [col["name"] for col in result_set.get("resultSetMetaData", {}).get("rowType", [])]
                        data = result_set.get("data", [])
                        if columns and data:
                            import pandas as pd
                            df = pd.DataFrame(data, columns=columns)
                            if len(df.index) > 1:
                                data_tab, line_tab, bar_tab = st.tabs(["Data", "Line Chart", "Bar Chart"])
                                data_tab.dataframe(df)
                                if len(df.columns) > 1:
                                    df_chart = df.set_index(df.columns[0])
                                else:
                                    df_chart = df
                                with line_tab:
                                    st.line_chart(df_chart)
                                with bar_tab:
                                    st.bar_chart(df_chart)
                            else:
                                st.dataframe(df)
                    if "text" in json_data:
                        st.markdown(json_data["text"])
        elif item["type"] == "tool_use":
            pass

def run_agent_tab():
    icons = {"assistant": "❄️", "user": "👤"}
    st.markdown("routes to Search or Analyst")

    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []
        st.session_state.agent_display_messages = []

    for msg in st.session_state.agent_display_messages:
        with st.chat_message(msg["role"], avatar=icons[msg["role"]]):
            if isinstance(msg["content"], str):
                st.markdown(msg["content"])
            else:
                display_agent_content(msg["content"])

    if question := st.chat_input("Ask anything about pizza...", key="agent_input"):
        st.session_state.agent_messages.append(
            {"role": "user", "content": [{"type": "text", "text": question}]}
        )
        st.session_state.agent_display_messages.append(
            {"role": "user", "content": question}
        )

        with st.chat_message("user", avatar=icons["user"]):
            st.markdown(question)

        with st.chat_message("assistant", avatar=icons["assistant"]):
            with st.spinner("Agent is thinking..."):
                resp = send_agent_message(st.session_state.agent_messages)
                content = resp.get("content", [])
                display_agent_content(content)

        st.session_state.agent_messages.append(
            {"role": "assistant", "content": content}
        )
        st.session_state.agent_display_messages.append(
            {"role": "assistant", "content": content}
        )

def main():
    """
    Main application function that orchestrates the Streamlit chatbot interface.
    
    This function sets up and runs the complete chatbot application:
    1. Displays the application title
    2. Initializes service metadata, configuration options, and message history
    3. Renders all previous chat messages with appropriate avatars
    4. Handles new user input through the chat interface
    5. Processes questions through the RAG pipeline and displays AI responses
    
    The chat interface is disabled if no Cortex Search Services are available.
    
    Flow:
        - User enters a question in the chat input
        - Question is added to message history and displayed with user avatar (👤)
        - System creates a RAG prompt with context from Cortex Search
        - AI generates a response using the selected model
        - Response is displayed with assistant avatar (❄️) and added to history
        
    Side Effects:
        - Renders the entire Streamlit UI
        - Modifies session state (messages list)
        - Executes queries against Snowflake Cortex services
        - Displays chat messages and responses in real-time
        
    Note:
        - Dollar signs ($) in questions are escaped to prevent Streamlit markdown issues
        - Single quotes (') are removed from questions before processing
        - Shows a "Thinking..." spinner while generating responses
    """
    st.title(f":speech_balloon: Pizza AI Assistant")
    tab1, tab2, tab3 = st.tabs(["Cortex Search", "Cortex Analyst","AI Agent"])

    with tab1:
        init_service_metadata()
        init_config_options()
        init_messages()
        st.subheader("Ask questions about Pizza Reviews")
        icons = {"assistant": "❄️", "user": "👤"}
        for message in st.session_state.messages:
            with st.chat_message(message["role"], avatar=icons[message["role"]]):
                st.markdown(message["content"])
        disable_chat = "service_metadata" not in st.session_state or len(st.session_state.service_metadata) == 0
        if question := st.chat_input("Ask a question...", disabled=disable_chat):
            st.session_state.messages.append({"role": "user", "content": question})
            with st.chat_message("user", avatar=icons["user"]):
                st.markdown(question.replace("$", "\$"))
            with st.chat_message("assistant", avatar=icons["assistant"]):
                message_placeholder = st.empty()
                question = question.replace("'", "")
                with st.spinner("Thinking..."):
                    generated_response = complete(st.session_state.model_name, create_prompt(question))
                    message_placeholder.markdown(generated_response)
            st.session_state.messages.append({"role": "assistant", "content": generated_response})
    with tab2:
        run_analyst_tab()
    with tab3:
        run_agent_tab()

if __name__ == "__main__":
    session = get_session()
    root = Root(session)
    main()
