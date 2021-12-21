import logging
logging.basicConfig()
from time import time, sleep
from threading import Thread
from kazoo.client import KazooClient as KClient


class Philosopher(Thread):
    def __init__(self, _node, _id, _path_to_forks, _eat_time = 2, _think_time = 1, _duration = 50):
        super().__init__()
        self.node = _node                                   # Путь до главного узла
        self.path_to_forks = _path_to_forks                 # путь до узла вилок
        self.id = _id                                       # Id философа
        self.left_fork_id = _id                             # Id левой вилки
        self.right_fork_id = _id + 1 if _id + 1 < 6 else 1  # Id правой вилки
        self.duration = _duration                           # Продолжительность приёма пищи философом
        self.eat_time = _eat_time                           # Время приёма пищи философом
        self.think_time = _think_time                       # Время размышления философа

    def run(self):
        kc = KClient()
        kc.start()

        table = kc.Lock(f'{self.node}/table', self.id)
        left_fork = kc.Lock(f'{self.node}/{self.path_to_forks}/{self.left_fork_id}', self.id)
        right_fork = kc.Lock(f'{self.node}/{self.path_to_forks}/{self.right_fork_id}', self.id)

        begin = time()
        print(f'Философ {self.id} размышляет.')
        while time() - begin < self.duration:          
            # Блокировка стола для проверки ближайших вилок
            with table:
                if len(left_fork.contenders()) == 0 and len(right_fork.contenders()) == 0:
                    # Если ближайшие вилки не заняты никем, то философ их забирает
                    left_fork.acquire()
                    right_fork.acquire()
            
            # Если у философа обе вилки, то он начинает кушать
            if left_fork.is_acquired:
                print(f'Философ {self.id} кушает.')
                sleep(self.eat_time)
                left_fork.release()
                right_fork.release()
                
                print(f'Философ {self.id} размышляет.')
                sleep(self.think_time)
                        
        kc.stop()
        kc.close()


if __name__ == "__main__":
    main_kc = KClient()
    main_kc.start()

    node = '/Philosophers'
    path_to_forks = '/forks'
    
    if main_kc.exists(node):
        main_kc.delete(node, recursive=True)

    main_kc.create(f'{node}')
    main_kc.create(f'{node}/table')
    main_kc.create(f'{node}{path_to_forks}')
    for i in range(1, 6):
        main_kc.create(f'{node}{path_to_forks}/{i}')

    for i in range(1, 6):
        p = Philosopher(node, i, path_to_forks, _eat_time=4, _think_time = 1, _duration=40)
        p.start()
