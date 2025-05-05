from langfuse import Langfuse
import uuid
langfuse = Langfuse(
    public_key="pk-lf-81438046-2135-4e6d-9d1b-531f8106fa3d",
    secret_key="sk-lf-8cfbce47-ede9-49bd-84b0-fbd061f640e4",
    host="http://localhost:3000"
)

# Tạo user_id và session_id
user_id = "user123"
session_id = str(uuid.uuid4())

# Tạo trace với user_id
trace = langfuse.trace(
    name="research-agent-trace",
    user_id=user_id,  # Thêm user_id
    session_id=session_id,  # Tùy chọn: thêm session_id
)