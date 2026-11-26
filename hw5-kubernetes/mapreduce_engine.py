import itertools
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed


def normalize_skills(value):
    if not value:
        return []
    return sorted({
        " ".join(skill.strip().split()).casefold()
        for skill in value.split(";")
        if skill.strip()
    })


def mapper_count(skill_values):
    singles = Counter()
    pairs = Counter()
    triples = Counter()

    for value in skill_values:
        skills = normalize_skills(value)
        singles.update(skills)
        pairs.update(itertools.combinations(skills, 2))
        triples.update(itertools.combinations(skills, 3))

    return {
        "singles": dict(singles),
        "pairs": dict(pairs),
        "triples": dict(triples),
    }


def reducer_count(mapped_results):
    reduced = {
        "singles": Counter(),
        "pairs": Counter(),
        "triples": Counter(),
    }
    for result in mapped_results:
        for kind in reduced:
            reduced[kind].update(result[kind])
    return {kind: dict(counter) for kind, counter in reduced.items()}


def run_with_retry(tasks, mapper, max_retries=1):
    results = {}
    for task_id, task in tasks:
        attempts = 0
        while True:
            try:
                results[task_id] = mapper((task_id, task))
                break
            except Exception:
                attempts += 1
                if attempts > max_retries:
                    raise
    return reducer_count(results[key] for key, _ in tasks)


def _mapper_attempt(skill_values, should_fail):
    if should_fail:
        raise RuntimeError("simulated mapper worker failure")
    return mapper_count(skill_values)


def run_parallel(tasks, workers=None):
    tasks = list(tasks)
    results = {}
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(mapper_count, values): task_id
            for task_id, values in tasks
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return reducer_count(results[key] for key, _ in tasks)


def run_parallel_with_retry(
    tasks, workers=None, max_retries=1, simulate_failure_for=None
):
    tasks = list(tasks)
    results = {}
    attempts = {task_id: 0 for task_id, _ in tasks}

    with ProcessPoolExecutor(max_workers=workers) as executor:
        active = {}
        for task_id, values in tasks:
            future = executor.submit(
                _mapper_attempt,
                values,
                task_id == simulate_failure_for,
            )
            active[future] = (task_id, values)

        while active:
            for future in as_completed(active):
                task_id, values = active.pop(future)
                try:
                    results[task_id] = future.result()
                except Exception:
                    if attempts[task_id] >= max_retries:
                        for pending in active:
                            pending.cancel()
                        raise
                    attempts[task_id] += 1
                    retry = executor.submit(_mapper_attempt, values, False)
                    active[retry] = (task_id, values)
                break

    return reducer_count(results[task_id] for task_id, _ in tasks)
