from app import app

if __name__=='__main__':    # from gevent.monkey import patch_all
    # patch=patch_all()

    # print(patch)

    from gevent.pywsgi import WSGIServer
    http_server=WSGIServer(('127.0.0.1',5000),app)
    print('app served')
    http_server.serve_forever()