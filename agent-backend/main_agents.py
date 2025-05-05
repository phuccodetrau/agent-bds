import asyncio
from manager import ResearchManager
import os
import base64
import uuid
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry import trace
import logfire

# Cấu hình Langfuse và OTLP
# os.environ["LANGFUSE_PUBLIC_KEY"] = "pk-lf-81438046-2135-4e6d-9d1b-531f8106fa3d"
# os.environ["LANGFUSE_SECRET_KEY"] = "sk-lf-8cfbce47-ede9-49bd-84b0-fbd061f640e4"
# os.environ["LANGFUSE_HOST"] = "http://localhost:3000"
# LANGFUSE_AUTH = base64.b64encode(
#     f"{os.environ.get('LANGFUSE_PUBLIC_KEY')}:{os.environ.get('LANGFUSE_SECRET_KEY')}".encode()
# ).decode()
#
# os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = os.environ.get("LANGFUSE_HOST") + "/api/public/otel"
# os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = f"Authorization=Basic {LANGFUSE_AUTH}"
#
# # Cấu hình OpenTelemetry
# trace_provider = TracerProvider()
# trace_provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
# trace.set_tracer_provider(trace_provider)
# tracer = trace.get_tracer(__name__)
#
# # Cấu hình Logfire
# logfire.configure(
#     service_name='my_agent_service',
#     send_to_logfire=False,
# )
# logfire.instrument_openai_agents()

async def main() -> None:
    # Tạo user_id và session_id
    # user_id = "user123"  # Thay bằng ID người dùng thực tế
    # session_id = "b4871930-e95c-4955-9246-4513eccf180a"
    #
    # # Bắt đầu một span và thêm user_id, session_id làm attribute
    # with tracer.start_as_current_span("research-session1") as span:
    #     span.set_attribute("user_id", user_id)  # Thêm user_id
    #     span.set_attribute("session_id", session_id)  # Thêm session_id
    query = input("What would you like to research? ")
    await ResearchManager().run(query)

if __name__ == "__main__":
    asyncio.run(main())