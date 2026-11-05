# Faisabilité dbt, DuckDB et Iceberg via Polaris

Résultats du laboratoire local du 5 novembre 2026. Les observations concernent
les versions et la configuration testées ; elles ne valident pas un remplacement
complet de Trino.

## Conclusion

La première création dbt sur Iceberg réussit. La reconstruction suivante
échoue parce que le chemin S3 de la table intermédiaire reste utilisé par la
table finale après renommage. Un chemin physique unique par invocation a
permis deux reconstructions successives, données relues et chemins vérifiés.

Cette surcharge ne suffit pas à garantir une publication protégée :
un échec de post-hook avec dbt 2.0.11 a laissé le nouveau résultat publié.
Un prototype séparé valide le candidat avant un échange transactionnel ;
les branches candidat invalide et candidat corrigé ont été vérifiées.

## Versions et accès

| Élément | Observé |
| --- | --- |
| dbt | 2.0.9 initialement, puis 2.0.11 |
| Moteur du pilote dbt | DuckDB 1.5.4, confirmé dans les traces du post-hook |
| Premiers tests Python | DuckDB 2.0.0 |
| Tests Python à deux connexions | DuckDB 1.5.4, environnement uv temporaire |
| Déploiement | Kind local, Polaris, SeaweedFS |
| Namespace d'écriture | `duckdb_feasibility_lab` |

Les révisions exactes des extensions et les versions Polaris/SeaweedFS restent
à relever et figer. Le changement de version dbt n'a pas été expliqué.
Les résultats des différentes versions ne doivent pas être confondus.

L'identité Floe a obtenu un token OAuth et accédé au catalogue. La lecture
de `sales_silver.orders` via Polaris a retourné 12 lignes. Depuis le Mac,
les essais utilisent des identifiants S3 distincts et
`ACCESS_DELEGATION_MODE 'none'` pour joindre le tunnel local.
Ce n'est pas une validation de délégation des droits S3 par Polaris.

## Résultats

| Test | Observation |
| --- | --- |
| Création Iceberg, renommage, lecture | Réussite |
| Deuxième lancement dbt sans surcharge | Refus 403 pour conflit d'emplacement |
| Chemin explicite à la création | Confirmé dans les métadonnées REST |
| Chemin unique par invocation | Deux reconstructions réussies, A=4/B=1 puis A=4/B=2 |
| Échec de conversion avant échange | Données A=4/B=2, UUID et snapshot inchangés |
| Échec de post-hook après échange, dbt 2.0.11 | Finale A=4/B=3 ; sauvegarde A=4/B=2, relues indépendamment |
| `DROP TABLE … CASCADE`, Python DuckDB 2.0.0 | Non pris en charge |
| Nettoyage dbt des reconstructions réussies | Suppression sans CASCADE observée ; sauvegarde absente du catalogue |
| Stockage après reconstruction | Métadonnées d'anciens lancements toujours présentes |
| Candidat qualité B=-1 | Arrêt avant renommage ; résultat publié A=4/B=2 préservé |
| Candidat corrigé B=3 puis publication | Finale A=5/B=3 ; précédente A=4/B=2, relues |

Le nom `dbt_customer_summary` désignait initialement le chemin physique
`…/dbt_customer_summary__dbt_tmp`. Le renommage ne déplaçait pas les fichiers.
Le lancement suivant tentait de réutiliser ce chemin déjà associé à une table.
Le correctif expérimental produit un chemin frère suffixé par
`invocation_id`, pas un sous-dossier du chemin occupé.

La macro de suppression du dbt testé possède déjà une branche sans
`CASCADE` pour Iceberg. L'erreur manuelle avec cette option ne signifie donc
pas qu'il faut systématiquement remplacer cette macro.

## Transactions

Des appels `run_query()` séparés à `BEGIN`, création ou renommage, puis
`ROLLBACK` ont échoué avec « no transaction is active », sur le stockage
local comme sur Iceberg. La cause exacte dans l'adaptateur reste à étudier.

Dans un seul lot SQL, le rollback d'un puis de deux renommages a laissé les
noms et contenus initiaux, vérifiés dans une nouvelle connexion.

Avec Python DuckDB 1.5.4 et deux connexions indépendantes :

| Moment | Écrivain | Lecteur indépendant |
| --- | --- | --- |
| Après échange, avant COMMIT | Nouvelle version | Ancienne version |
| Après COMMIT | Transaction terminée | Nouvelle version |

Ces observations ponctuelles ne démontrent ni disponibilité continue sous
charge, ni récupération après crash, ni garanties identiques pour dbt.
Le test de post-hook montre précisément que les garanties Python ne peuvent
pas être attribuées à la matérialisation dbt telle que testée.

## Limites

- Le remplacement par nouvelle table change son UUID et sa lignée de snapshots.
  Un nom stable ne conserve pas automatiquement l'historique Iceberg.
- La validation qualité précède la transaction : le candidat doit rester stable
  entre contrôle et publication. Aucun scénario multi-écrivain n'est validé.
- Le lot de publication ne démontre pas la récupération après une erreur
  entre renommages ou une coupure pendant COMMIT.
- D'anciens fichiers restent stockés. Leur présence ne suffit pas à les déclarer
  orphelins ; aucun inventaire complet ni nettoyage S3 n'a été réalisé.
- `CREATE OR REPLACE` n'a pas été testé directement.
- Aucun benchmark comparatif Trino ni validation complète ACID ou lineage.
- Les macros sont des probes dépendant de l'état, pas une suite complète.
  La copie regroupée pour publication nécessite encore une réexécution dédiée.

## Pièces de preuve

Les macros et modèles de ce dossier correspondent aux expériences.
Les journaux locaux de reconstructions montrent la création intermédiaire,
les deux renommages et le nettoyage. Le journal du post-hook montre les deux
renommages réussis avant l'erreur volontaire. Les lectures REST et SQL partagées
pendant la séance ont confirmé les identités, chemins et résultats ci-dessus.

Les journaux bruts ne sont pas publiés : ils peuvent contenir des informations
d'initialisation sensibles. Tous les scénarios Python et toutes les sorties REST
ne sont pas encore archivés comme tests exécutables.

## Prochaines étapes

1. Figer versions du CLI, moteur, extensions, catalogue et stockage.
2. Archiver des preuves expurgées et rendre les scénarios rejouables.
3. Examiner les transactions de la matérialisation et les options catalogue
   officielles avant de figer les contournements par hooks.
4. Tester erreurs au milieu de l'échange, coupures au commit et récupération.
5. Comparer les stratégies préservant l'identité de table et tester
   `CREATE OR REPLACE` explicitement.
6. Définir rétention, nettoyage sûr, identité dbt minimale et lineage.

## Lecture proposée pour Alexis

La surcharge de chemin constitue une preuve de faisabilité limitée pour les
reconstructions locales. La publication sur erreur, la continuité d'historique
et le cycle de vie des fichiers restent des travaux nécessaires avant toute
proposition de remplacement de la couche Gold.
