

class Batcher:

    def list_to_list_batch(self, batch_limit, iterator):
        batches = []

        current_batch = []
        for obj in iterator:
            if len(current_batch) < batch_limit:
                current_batch.append(obj)
            else:
                batches.append(current_batch)
                current_batch = [obj]

        batches.append(current_batch)

        return batches
