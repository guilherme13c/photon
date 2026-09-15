from pathlib import Path


def test_compose_limits_embedder_thread_oversubscription():
    compose = Path(__file__).parents[2].joinpath("docker-compose.yml").read_text()
    assert "OMP_NUM_THREADS: ${PHOTON_EMBED_OMP_THREADS:-2}" in compose
    assert "TOKENIZERS_PARALLELISM: ${PHOTON_TOKENIZERS_PARALLELISM:-false}" in compose
