using './retained-v050.bicep'

param location = 'francecentral'
param resourceGroupName = 'rg-overmesh-v050-live'
param uniqueSuffix = '8152352'
param operatorPrincipalId = 'a0ede19f-e63d-4e5c-aa3b-d2958be4febd'
param sshPublicKey = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKiqgqzz03I2wcswnJ/ZTN5bTDrZR7c8qfKp0lRa0y5t overmesh-v050-live'
param adminUsername = 'overmesh'
