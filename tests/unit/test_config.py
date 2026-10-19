import pytest

from xm_demanda.utils.config import fqn, load_config, validar_config


def _validar(cfg):
    try:
        return validar_config(cfg)
    except NotImplementedError:
        pytest.skip("Pendiente: lab 8, sección 2")


def test_fqn_dev():
    cfg = load_config("dev")
    assert fqn(cfg, "silver", "silver_diaria") == "dev.silver_energia.demanda_diaria"


@pytest.mark.parametrize("env", ["dev", "qa", "prod"])
def test_solo_cambia_el_ambiente(env):
    """Entre ambientes cambian catálogo y rutas; modelo y nombres de tablas son los mismos."""
    base, cfg = load_config("dev"), load_config(env)
    assert cfg["catalog"] == env
    assert cfg["modelo"] == base["modelo"] and cfg["tablas"] == base["tablas"]
    assert cfg["volumen_landing"].startswith(f"/Volumes/{env}/")


@pytest.mark.parametrize("env", ["dev", "qa", "prod"])
def test_config_real_es_valida(env):
    _validar(load_config(env))


def test_validar_detecta_llave_faltante():
    cfg = load_config("dev")
    del cfg["modelo"]
    with pytest.raises(ValueError, match="modelo"):
        _validar(cfg)


@pytest.mark.parametrize("llave", ["db_password", "TOKEN", "api_key", "storage_secret"])
def test_validar_rechaza_secretos_en_texto_plano(llave):
    cfg = load_config("dev")
    cfg["conexion"] = {llave: "no-deberia-estar-aqui"}
    with pytest.raises(ValueError, match="secreto"):
        _validar(cfg)
