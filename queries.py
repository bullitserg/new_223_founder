check_catalog_query = '''SELECT
  p.id
FROM procedure_223 p
WHERE p.registration_number = '%s'
AND p.version = '%s'
AND p.deleted_at IS NULL
;'''


check_old_223_query = '''SELECT
  a.id
FROM auction a
WHERE a.oosNotificationNumber = '%s'
AND a.actualId IS NULL
AND a.statusId = 0
;'''
