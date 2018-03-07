from dwcontents.api import DwContentsApi

def test_get_datasets():
    api = DwContentsApi('eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50OnJmbHByciIsImlzcyI6ImFnZW50OnJmbHBycjo6OTZmM2VlMGMtNzUzMi00Zjc3LWI0OWQtNmU1ZDY5MDZhYWJjIiwiaWF0IjoxNDg2NTA3OTU1LCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWV9.RxIZyvpi9K5zIRWoolgYq3U3c2mhvkc60wgVvAzaPbh7te6OgFCRYgvZiMuz-jQXAd9fO_2JgwHJbaWuPnaGUQ')
    datasets = api.get_datasets()
    print(['{}/{}'.format(d['owner'], d['title']) for d in datasets])