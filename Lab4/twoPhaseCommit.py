import logging
logging.basicConfig()

from threading import Thread, Timer
from time import sleep, time

from numpy.random import random

from kazoo.client import KazooClient as KClient


class Participant(Thread):
    def __init__(self, path, _id, decision_time=4, true_probability=0.5):
        super().__init__()
        self.node = f'{path}/{_id}'
        self.id = _id
        self.true_probability = true_probability
        self.decision_time = decision_time

    def run(self):
        participant = KClient()
        participant.start()
        
        sleep(self.decision_time)
        
        decision = b'commit' if random() < self.true_probability else b'abort'
        print(f'Участник №{self.id}: Моё решение {decision.decode()}')
        participant.create(self.node, decision, ephemeral=True)
        
        @participant.DataWatch(self.node)
        def watch_myself(data, stat):
            if stat.version != 0:
                print(f'Участник №{self.id}: Принял ответ от координатора: {data.decode()}')

        # Ожидание ответа от координатора перед остановкой 
        sleep(10)
        
        participant.stop()
        participant.close()


class Coordinator():
    def main(self, root, num_partisipants=5, duration=30):
        coordinator = KClient()
        coordinator.start()

        if coordinator.exists(root):
            coordinator.delete(root, recursive=True)

        coordinator.create(root)
        partisipants_node = f'{root}/partisipants'
        coordinator.create(partisipants_node)

        self.timer = None

        def make_main_decision():
            partisipants = coordinator.get_children(partisipants_node)
            decisions = []
            for partisipant in partisipants:
                decisions.append(coordinator.get(f'{partisipants_node}/{partisipant}')[0] == b'commit')

            # all(array) - вернет True, если все элементы массива истинны, иначе False
            main_decision = b'commit' if all(decisions) else b'abort'
            for partisipant in partisipants:
                coordinator.set(f'{partisipants_node}/{partisipant}', main_decision)

        @coordinator.ChildrenWatch(partisipants_node)
        def watch_partisipants(partisipants):
            if self.timer is not None:
                self.timer.cancel()
            if len(partisipants) != 0:
                self.timer = Timer(duration, make_main_decision)
                self.timer.daemon = True
                self.timer.start()

            if len(partisipants) == num_partisipants:
                print('Координатор: Принятие главного решения...')
                self.timer.cancel()
                make_main_decision()

        for i in range(num_partisipants):
            p = Participant(partisipants_node, i, decision_time=3, true_probability=0.8)
            p.start()


if __name__ == "__main__":
    root = '/coordinator'
    duration = 15
    Coordinator().main(root, duration=duration)
