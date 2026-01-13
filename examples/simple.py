import coral

image = (
    coral.Image.python("python:3.11-slim")
    .apt_install("git")
    .pip_install("requests")
    .env({"EXAMPLE_ENV": "1"})
)

app = coral.App(name="simple", image=image)


@app.function(cpu=1, memory="1Gi", timeout=300)
def process(text: str) -> dict:
    tokens = text.split()
    return {"words": len(tokens), "upper": text.upper()}


@app.local_entrypoint()
def main():
    result = process.remote("hello coral")
    print("result:", result)
