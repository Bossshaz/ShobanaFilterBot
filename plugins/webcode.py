from aiohttp import web as webserver
from plugins.stream_server import stream_routes

routes = webserver.RouteTableDef()

async def bot_run():
    _app = webserver.Application(client_max_size=30000000)
    _app.add_routes(routes)
    _app.add_routes(stream_routes)
    return _app

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return webserver.json_response("Web Supported . . . ! This is a preview of WeB . . . !!!")
