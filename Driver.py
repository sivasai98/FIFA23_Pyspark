from src.Module.app import App


def main():
    with App() as app:
        app.do()


if __name__ == '__main__':
    main()
