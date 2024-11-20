from app_init import app
from controller.main_controller import app_layout


if __name__ == "__main__":
    app.layout = app_layout
    app.run_server(debug=True, host='0.0.0.0', port=8050)
