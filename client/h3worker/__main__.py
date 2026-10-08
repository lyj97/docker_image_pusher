"""python -m h3worker entry point."""

from .worker import main

if __name__ == "__main__":
    try:
        main()
    except Exception:
        # The shell's EXIT trap cannot survive exec. Preserve reporting for
        # configuration or early startup failures in the new Worker process.
        from .startup_prepare import main as preparation_main
        try:
            preparation_main()
        except (Exception, SystemExit):
            pass
        raise
