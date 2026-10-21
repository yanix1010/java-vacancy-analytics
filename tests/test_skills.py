import itertools
from collections import Counter

def normalize_skills(value):
    if not value:
        return []
    return sorted({
        " ".join(skill.strip().split()).casefold()
        for skill in value.split(";")
        if skill.strip()
    })

def count_combinations(value):
    skills = normalize_skills(value)
    return {
        "singles": Counter(skills),
        "pairs": Counter(itertools.combinations(skills, 2)),
        "triples": Counter(itertools.combinations(skills, 3)),
    }

def test_normalize_skills():
    assert normalize_skills(" Java ; Python; Java ;  SQL ") == [
        "java", "python", "sql"
    ]

def test_empty_skills():
    assert normalize_skills("") == []
    assert normalize_skills(None) == []

def test_pairs_and_triples():
    result = count_combinations("Java; Python; SQL")
    assert result["singles"] == Counter(["java", "python", "sql"])
    assert result["pairs"] == Counter({
        ("java", "python"): 1,
        ("java", "sql"): 1,
        ("python", "sql"): 1,
    })
    assert result["triples"] == Counter({
        ("java", "python", "sql"): 1,
    })

def test_duplicate_skill_does_not_inflate_counts():
    result = count_combinations("Java;Java;Python;Python")
    assert result["singles"] == Counter(["java", "python"])
    assert result["pairs"] == Counter({("java", "python"): 1})
    assert result["triples"] == Counter()
