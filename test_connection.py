# from google.cloud import pubsub_v1
# from config.settings import settings

# publisher = pubsub_v1.PublisherClient()
# topic_path = publisher.topic_path(
#     settings.gcp_project_id, 
#     settings.pubsub_topic
# )
# print(f"Topic path: {topic_path}")
# print("Connection successful!")


from google.auth import default

creds, project = default()
print("AUTH PROJECT:", project)
print("CREDS TYPE:", type(creds))