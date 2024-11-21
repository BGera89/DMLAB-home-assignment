# Example Multipage Support in index.py
from dash import dcc, html
from dash.dependencies import Input, Output
from app_init import app
from controller.main_controller import app_layout


# Define a default layout that includes a placeholder for all pages
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([], id='page-content'),
    # Add all components here, even if they are hidden initially
])


@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/':
        return app_layout  # Example other page
    else:
        # Dynamically return the home screen
        return html.Div([html.H1("Page Not Found")])


if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0', port=8050)
