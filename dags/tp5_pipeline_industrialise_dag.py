"""
TP 5 — Pipeline industrialisé Open-Meteo

Workflow :
  extraire_donnees_meteo
      >> archiver_donnees_brutes
      >> transformer_donnees
      >> controler_qualite_donnees
      >> decider_branchement
            |-- (qualité OK)  --> charger_postgresql --+
            |-- (qualité KO)  --> tracer_anomalie_qualite --+
            +-------------------->> journaliser_execution >> exporter_preuves
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from meteo.tp5.archivage import archiver_donnees_brutes
from meteo.tp5.config import CONNEXION_POSTGRES_DEFAUT, VILLES_PAR_DEFAUT, normaliser_villes
from meteo.tp5.extraction import extraire_donnees_meteo
from meteo.tp5.postgres import (
    charger_meteo_idempotent,
    compter_lignes_meteo,
    initialiser_schema,
)
from meteo.tp5.qualite import controler_qualite
from meteo.tp5.tracabilite import (
    enregistrer_anomalies,
    exporter_preuve_fichier,
    journaliser_execution,
)
from meteo.tp5.transformation import transformer_donnees

XCOM_QUALITE = "resultat_qualite"
XCOM_ARCHIVE = "chemin_archive"


def _conf(context: dict) -> dict:
    return (context.get("dag_run") and context["dag_run"].conf) or {}


def _param(context: dict, cle: str, defaut):
    return _conf(context).get(cle, context["params"][cle])


def _villes(context: dict) -> list[dict]:
    return normaliser_villes(_param(context, "villes", VILLES_PAR_DEFAUT))


def _connexion(context: dict) -> str:
    return _param(context, "connexion_postgres", CONNEXION_POSTGRES_DEFAUT)


def _simuler_anomalie(context: dict) -> bool:
    return bool(_param(context, "simuler_anomalie_qualite", False))


def tache_extraire(**context) -> list[dict]:
    return extraire_donnees_meteo(_villes(context))


def tache_archiver(**context) -> str:
    brutes = context["ti"].xcom_pull(task_ids="extraire_donnees_meteo")
    return archiver_donnees_brutes(brutes, context["dag_run"].run_id)


def tache_transformer(**context) -> list[dict]:
    brutes = context["ti"].xcom_pull(task_ids="extraire_donnees_meteo")
    donnees = transformer_donnees(brutes, simuler_anomalie=_simuler_anomalie(context))
    for ligne in donnees:
        ligne["dag_run_id"] = context["dag_run"].run_id
    return donnees


def tache_controler_qualite(**context) -> dict:
    donnees = context["ti"].xcom_pull(task_ids="transformer_donnees")
    return controler_qualite(donnees)


def tache_decider_branchement(**context) -> str:
    resultat = context["ti"].xcom_pull(task_ids="controler_qualite_donnees")
    if resultat["valide"]:
        return "charger_postgresql"
    return "tracer_anomalie_qualite"


def tache_charger_postgresql(**context) -> int:
    connexion = _connexion(context)
    resultat = context["ti"].xcom_pull(task_ids="controler_qualite_donnees")
    initialiser_schema(connexion)
    return charger_meteo_idempotent(connexion, resultat["donnees"])


def tache_tracer_anomalie(**context) -> None:
    connexion = _connexion(context)
    resultat = context["ti"].xcom_pull(task_ids="controler_qualite_donnees")
    initialiser_schema(connexion)
    enregistrer_anomalies(connexion, context["dag_run"].run_id, resultat["anomalies"])


def tache_journaliser(**context) -> None:
    connexion = _connexion(context)
    resultat = context["ti"].xcom_pull(task_ids="controler_qualite_donnees")
    chemin_archive = context["ti"].xcom_pull(task_ids="archiver_donnees_brutes")
    villes = [v["ville"] for v in _villes(context)]
    simuler = _simuler_anomalie(context)

    if resultat["valide"]:
        nb_lignes = context["ti"].xcom_pull(task_ids="charger_postgresql") or 0
        statut = "success"
    else:
        nb_lignes = 0
        statut = "anomalie_qualite"

    mode = "anomalie_simulee" if simuler else "nominal"
    journaliser_execution(
        connexion,
        context["dag_run"].run_id,
        statut,
        nb_lignes,
        villes,
        chemin_archive,
        nb_anomalies=resultat["nb_anomalies"],
        mode_execution=mode,
    )


def tache_exporter_preuves(**context) -> None:
    connexion = _connexion(context)
    idempotence = compter_lignes_meteo(connexion)
    simuler = _simuler_anomalie(context)
    suffixe = "anomalie" if simuler else "nominal"
    exporter_preuve_fichier(
        connexion,
        f"preuve_{suffixe}_{context['dag_run'].run_id.replace(':', '_')}.json",
        extra={"idempotence": idempotence, "simuler_anomalie": simuler},
    )


default_args = {
    "owner": "lyesatb",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="tp5_pipeline_industrialise",
    default_args=default_args,
    description="TP5 — Pipeline industrialisé Open-Meteo avec qualité et idempotence",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tp5", "meteo", "industrialisation"],
    params={
        "villes": Param(
            default=VILLES_PAR_DEFAUT,
            type="array",
            description="Villes à traiter",
        ),
        "connexion_postgres": Param(
            default=CONNEXION_POSTGRES_DEFAUT,
            type="string",
            description="URL PostgreSQL",
        ),
        "simuler_anomalie_qualite": Param(
            default=False,
            type="boolean",
            description="True = injecte une anomalie qualité (cas test)",
        ),
    },
) as dag:

    extraire = PythonOperator(
        task_id="extraire_donnees_meteo",
        python_callable=tache_extraire,
        execution_timeout=timedelta(minutes=5),
    )

    archiver = PythonOperator(
        task_id="archiver_donnees_brutes",
        python_callable=tache_archiver,
    )

    transformer = PythonOperator(
        task_id="transformer_donnees",
        python_callable=tache_transformer,
    )

    controler = PythonOperator(
        task_id="controler_qualite_donnees",
        python_callable=tache_controler_qualite,
    )

    decider = BranchPythonOperator(
        task_id="decider_branchement",
        python_callable=tache_decider_branchement,
    )

    charger = PythonOperator(
        task_id="charger_postgresql",
        python_callable=tache_charger_postgresql,
    )

    tracer = PythonOperator(
        task_id="tracer_anomalie_qualite",
        python_callable=tache_tracer_anomalie,
    )

    journaliser = PythonOperator(
        task_id="journaliser_execution",
        python_callable=tache_journaliser,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    exporter = PythonOperator(
        task_id="exporter_preuves",
        python_callable=tache_exporter_preuves,
    )

    extraire >> archiver >> transformer >> controler >> decider
    decider >> charger >> journaliser
    decider >> tracer >> journaliser
    journaliser >> exporter
