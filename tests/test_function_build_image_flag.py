from __future__ import annotations

import coral


def test_function_build_image_defaults_true() -> None:
    app = coral.App(name="build-image-default")

    @app.function()
    def process() -> int:
        return 1

    assert app.get_function("process").spec.build_image is True


def test_function_build_image_can_be_disabled() -> None:
    app = coral.App(name="build-image-disabled")

    @app.function(build_image=False)
    def process() -> int:
        return 1

    assert app.get_function("process").spec.build_image is False
