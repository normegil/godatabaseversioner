package godatabaseversioner

import (
	"fmt"
	"sort"
)

// EventType represents a type of event that can be raised during version syncing
type EventType string

const (
	EventStart             EventType = "start"
	EventEnd               EventType = "end"
	EventBeforeSync        EventType = "before-sync"
	EventAfterSync         EventType = "after-sync"
	EventBeforeChange      EventType = "before-change"
	EventAfterChange       EventType = "after-change"
	EventErrorDuringChange EventType = "error-during-change"
	EventError             EventType = "error"
)

// Event represents an event during version syncing
type Event struct {
	Type    EventType
	Version Version
	Error   error
}

// Listener allow to react to some event during initialization
type Listener interface {
	// On should implement the logic to execute when an event is triggered
	On(event Event) error
}

// Version hold the scripts that will allow a structure version to be upgraded or downgraded
type Version interface {
	// Number should return the version number for these modifications. Version numbers don't need to be follow a
	// successive number series, but the highest the number, the more up-to-date a structure is. Start at 0, 0 being an
	// empty state (without version)
	Number() int
	// Upgrade will hold the script use to upgrade database version
	Upgrade() error
	// Rollback will allow to rollback the database to a previous state
	Rollback() error
}

// VersionerQuerier is used to manage operations of Versioner on a specific type of structure
type VersionApplier interface {
	// CurrentVersion return current structure version
	CurrentVersion() (int, error)
	// SyncVersion should upgrade stored version for concerned structure
	SyncVersion(versionNb int) error
}

// Versioner hold the logic behind upgrade/downgrade of the managed structure
type Versioner struct {
	Applier  VersionApplier
	Versions []Version
	Listener Listener
}

// NewVersioner will create a versioner without any listener
func NewVersioner(applier VersionApplier, Versions []Version) *Versioner {
	return &Versioner{
		Applier:  applier,
		Versions: Versions,
		Listener: NoOpListener{},
	}
}

// Current version will return current structure version without applying any modification
func (v Versioner) CurrentVersion() (int, error) {
	return v.Applier.CurrentVersion()
}

// Last version will return last applicable version, based on assigned versions
func (v Versioner) LastVersion() int {
	versionNb := 0
	for _, version := range v.Versions {
		number := version.Number()
		if number > versionNb {
			versionNb = number
		}
	}
	return versionNb
}

// UpgradeToLast will upgrade the structure to the last available version
func (v Versioner) UpgradeToLast() error {
	return v.Sync(v.LastVersion())
}

// Sync will sync the structure to specified version
func (v Versioner) Sync(targetVersion int) error {
	if err := v.Listener.On(Event{EventStart, nil, nil}); nil != err {
		return fmt.Errorf("event %s: %w", EventStart, err)
	}
	currentVersion, err := v.CurrentVersion()
	if err != nil {
		if eventErr := v.Listener.On(Event{EventError, nil, err}); nil != eventErr {
			return fmt.Errorf("could not sync (event error: %s): %w", eventErr.Error(), err)
		}
		return fmt.Errorf("could not sync: %w", err)
	}

	if currentVersion == targetVersion {
		if err := v.Listener.On(Event{EventEnd, nil, nil}); nil != err {
			return fmt.Errorf("event %s: %w", EventEnd, err)
		}
		return nil
	}

	sort.Slice(v.Versions, func(i, j int) bool {
		return v.Versions[i].Number() < v.Versions[j].Number()
	})

	upgrade := true
	if targetVersion < currentVersion {
		upgrade = false
	}
	versionsToApply := v.loadVersionsToApply(upgrade, currentVersion, targetVersion)

	if err := v.Listener.On(Event{EventBeforeSync, nil, nil}); nil != err {
		return fmt.Errorf("event %s: %w", EventBeforeSync, err)
	}
	for _, version := range versionsToApply {
		if err := v.Listener.On(Event{EventBeforeChange, version, nil}); nil != err {
			return fmt.Errorf("event %s: %w", EventBeforeChange, err)
		}
		if upgrade {
			if err := version.Upgrade(); nil != err {
				if eventErr := v.Listener.On(Event{EventErrorDuringChange, version, err}); nil != eventErr {
					return fmt.Errorf("upgrade to version %d (event error: %s): %w", version.Number(), eventErr.Error(), err)
				}
				return fmt.Errorf("upgrade to version %d: %w", version.Number(), err)
			}
		} else {
			if err := version.Rollback(); nil != err {
				if eventErr := v.Listener.On(Event{EventErrorDuringChange, version, err}); nil != eventErr {
					return fmt.Errorf("rollback to version %d (event error: %s): %w", version.Number(), eventErr.Error(), err)
				}
				return fmt.Errorf("rollback to version %d: %w", version.Number(), err)
			}
		}
		if err := v.Applier.SyncVersion(version.Number()); nil != err {
			if eventErr := v.Listener.On(Event{EventErrorDuringChange, version, err}); nil != eventErr {
				return fmt.Errorf("sync version to %d (event error: %s): %w", version.Number(), eventErr.Error(), err)
			}
			return fmt.Errorf("sync version to %d: %w", version.Number(), err)
		}
		if err := v.Listener.On(Event{EventAfterChange, version, nil}); nil != err {
			return fmt.Errorf("event %s: %w", EventAfterChange, err)
		}
	}
	if err := v.Listener.On(Event{EventAfterSync, nil, nil}); nil != err {
		return fmt.Errorf("event %s: %w", EventAfterSync, err)
	}

	if err := v.Listener.On(Event{EventEnd, nil, nil}); nil != err {
		return fmt.Errorf("event %s: %w", EventEnd, err)
	}
	return nil
}

func (v Versioner) loadVersionsToApply(upgrade bool, currentVersion int, targetVersion int) []Version {
	toApply := make([]Version, 0)
	for _, version := range v.Versions {
		if upgrade {
			isBetweenCurrentAndTargetVersions := version.Number() > currentVersion && version.Number() < targetVersion
			if isBetweenCurrentAndTargetVersions {
				toApply = append(toApply, version)
			}
		} else {
			isBetweenCurrentAndTargetVersions := version.Number() < currentVersion && version.Number() > targetVersion
			if isBetweenCurrentAndTargetVersions {
				toApply = append([]Version{version}, toApply...)
			}
		}
	}
	return toApply
}
