# Copyright 2013 Google Inc. All Rights Reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compute Engine API data tranform stage unit tests."""



from src import basetest
from src.pipelines.stages import gcedatatransformer


class GceDataTransformerTest(basetest.TestCase):

  def testAddNamePropertyToUrlProperty(self):
    # Set up properties with two values.
    properties = {'status': 'RUNNING',
                  'machineType': 'https://www.googleapis.com/compute/v1/'
                                 'projects/google.com:cloud-gce/zones/'
                                 'europe-west1-b/machineTypes/n1-standard-4'
                 }

    # Should add a name property to machineType
    gcedatatransformer.GceDataTransformer._AddNamePropertyToUrlProperty(
        properties, 'machineType')
    self.assertIn('machineTypeName', properties)
    self.assertEqual('n1-standard-4', properties.get('machineTypeName'))
    self.assertEqual(3, len(properties))

    # Should not add a name property to status
    gcedatatransformer.GceDataTransformer._AddNamePropertyToUrlProperty(
        properties, 'status')
    self.assertNotIn('statusName', properties)
    self.assertEqual(3, len(properties))

  def testTransformInstanceDataServiceAccount(self):
    # Convert the scopes from an array to a comma separated list.
    email_scope = 'https://www.googleapis.com/auth/userinfo.email'
    compute_scope = 'https://www.googleapis.com/auth/compute'
    storage_scope = 'https://www.googleapis.com/auth/devstorage.full_control'

    instance = {'status': 'RUNNING',
                'serviceAccounts': [
                    {'scopes': [email_scope, compute_scope, storage_scope],
                     'email': '987409142022@project.gserviceaccount.com'
                    }]
               }

    gcedatatransformer.GceDataTransformer._TransformInstanceData(instance)
    self.assertEqual('%s,%s,%s' % (email_scope, compute_scope, storage_scope),
                     instance['serviceAccounts'][0]['scopes'])

    # Convert the scopes from an array to a comma separated list
    # with multiple scopes
    instance = {'status': 'RUNNING',
                'serviceAccounts': [
                    {'scopes': [email_scope, compute_scope, storage_scope],
                     'email': '888888888888@project.gserviceaccount.com'
                    },
                    {'scopes': [compute_scope],
                     'email': '999999999999@project.gserviceaccount.com'
                    }]
               }
    gcedatatransformer.GceDataTransformer._TransformInstanceData(instance)
    self.assertEqual('%s,%s,%s' % (email_scope, compute_scope, storage_scope),
                     instance['serviceAccounts'][0]['scopes'])
    self.assertEqual(compute_scope, instance['serviceAccounts'][1]['scopes'])

  def testTransformInstanceDataTagItems(self):
    # Converts the items in tags from an array to a repeated field
    instance = {'status': 'RUNNING',
                'tags': {'items': ['cassandra'],
                         'fingerprint': '4ndtHne7-vE='}
               }
    gcedatatransformer.GceDataTransformer._TransformInstanceData(instance)
    self.assertListEqual([{'item': 'cassandra'}], instance['tags']['items'])
    # Test with multiple items
    instance = {'status': 'RUNNING',
                'tags': {'items': ['cassandra', 'mongoDb'],
                         'fingerprint': '4ndtHne7-vE='}
               }
    gcedatatransformer.GceDataTransformer._TransformInstanceData(instance)
    self.assertListEqual([{'item': 'cassandra'}, {'item': 'mongoDb'}],
                         instance['tags']['items'])


if __name__ == '__main__':
  basetest.main()
