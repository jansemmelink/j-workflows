# Workflows #

Workflows are essentially slow running processes consisting of multiple steps and business logic.

This service implements a framework to execute such processes by calling a workflow function which calls and yields to actions. When the action is completed the workflow function is called again, skipping over the completed actions to re-run the business logic until it yields to a next action. This repeats until the workflow completes or fails.

# Status #

This is work in progress. It is based on what I saw done in Python using python's built-in yield mechanism, which I then wanted to try in Go.

## Done ##

* A simple framework to run workflows using channels and a yield mechanism with an example, running as a standalone process.

## Todo ##
* Store persistent session data externally and use an external queue service for events, so that multiple instances can be used and continue workflows started in other instances which might have terminator.
* Deal with workflow upgrades without breaking running workflows - e.g. by subscribing to version specific topics, but also consider how a stuck running workflow can be fixed, e.g. by subscribing to a major.minor, but allow patches to be created and continue the same workflow, and stop older versions so only patched versions can resume...
* Start sub workflows and wait for them to complete
* Start group of sub workflows/actions parallel and wait for all to complete
* Deal with action errors and provide retry mechanism and control
* Custom audit writer and interfaces for sessions and events.
* Limit execution time on actions/sub workflows
* Limit execution time on the workflow function (excluding action time)
* Log complete workflow for replay and visualisation
* Provide metrics
* Add use of scheduled events - probably as another external service.
* Pipeline deployment in AWS
