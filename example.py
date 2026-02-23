import coral

image = (
    coral.Image.python("pytorch/pytorch:2.10.0-cuda12.8-cudnn9-runtime")
    .pip_install("requests")
    .env({"EXAMPLE_ENV": "1"})
)

app = coral.App(name="example", image=image)


@app.function(cpu=1, memory="2Gi", timeout=300)
def process(text: str) -> dict:
    tokens = text.split()
    return {"words": len(tokens), "upper": text.upper()}


@app.local_entrypoint()
def main():
    result = process.remote("hello coral")
    print("result:", result)
