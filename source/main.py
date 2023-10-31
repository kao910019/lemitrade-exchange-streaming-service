import asyncio
import signal

from libs.configs import Configs
from libs.utils import *

from streamingManagerService import StreamingManagerService
from settings import *

# ====================================== APP Main ======================================

async def RunExchangeStreamService():
    logging.info('Start: {} - {}'.format(__name__, SYSTEM_TIME()))
    logging.info('System: {}'.format(SYSTEM_PLATFORM))
    logging.info(LOGO)
    configs = Configs(CONFIGS_FILE)
    service = StreamingManagerService(configs)
    
    try:
        await service.Start()
    except Exception as e:
        logging.critical("Service Start Error:", e)
        if SYSTEM_PLATFORM == "Linux":
            os.kill(1, signal.SIGINT)
    
    try:
        while service.info["state"] != "stop":
            await service.Loop()
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass

    await service.Close()
    logging.info('END: {} - {}'.format(__name__, SYSTEM_TIME()))
    # Closing Process for Drone
    if SYSTEM_PLATFORM == "Linux":
        os.kill(1, signal.SIGTERM)

if __name__ == "__main__":
    asyncio.run(RunExchangeStreamService())