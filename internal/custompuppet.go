package internal

import (
	"context"
	"maunium.net/go/mautrix/id"
)

func (p *Puppet) SwitchCustomMXID(accessToken string, mxid id.UserID) error {
	ctx := context.Background()
	p.CustomMXID = mxid
	p.AccessToken = accessToken
	p.EnablePresence = p.bridge.Config.Bridge.DefaultBridgePresence
	p.Update(ctx)
	err := p.StartCustomMXID(ctx, false)
	if err != nil {
		return err
	}
	// TODO leave rooms with default puppet
	return nil
}

func (p *Puppet) ClearCustomMXID() {
	save := p.CustomMXID != "" || p.AccessToken != ""
	p.bridge.puppetsLock.Lock()
	if p.CustomMXID != "" && p.bridge.puppetsByCustomMXID[p.CustomMXID] == p {
		delete(p.bridge.puppetsByCustomMXID, p.CustomMXID)
	}
	p.bridge.puppetsLock.Unlock()
	p.CustomMXID = ""
	p.AccessToken = ""
	p.customIntent = nil
	p.customUser = nil
	if save {
		p.Update(context.Background())
	}
}

func (p *Puppet) StartCustomMXID(ctx context.Context, reloginOnFail bool) error {
	newIntent, newAccessToken, err := p.bridge.DoublePuppet.Setup(ctx, p.CustomMXID, p.AccessToken, reloginOnFail)
	if err != nil {
		p.ClearCustomMXID()
		return err
	}
	p.bridge.puppetsLock.Lock()
	p.bridge.puppetsByCustomMXID[p.CustomMXID] = p
	p.bridge.puppetsLock.Unlock()
	if p.AccessToken != newAccessToken {
		p.AccessToken = newAccessToken
		p.Update(ctx)
	}
	p.customIntent = newIntent
	p.customUser = p.bridge.GetUserByMXID(ctx, p.CustomMXID)
	return nil
}

func (u *User) tryAutomaticDoublePuppeting(ctx context.Context) {
	if !u.bridge.Config.CanAutoDoublePuppet(u.MXID) {
		return
	}
	u.log.Debug().Msgf("Checking if double puppeting needs to be enabled")
	puppet := u.bridge.GetPuppetByUID(ctx, u.UID)
	if len(puppet.CustomMXID) > 0 {
		u.log.Debug().Msgf("User already has double-puppeting enabled")
		// Custom puppet already enabled
		return
	}
	puppet.CustomMXID = u.MXID
	puppet.EnablePresence = puppet.bridge.Config.Bridge.DefaultBridgePresence
	err := puppet.StartCustomMXID(ctx, true)
	if err != nil {
		u.log.Warn().Err(err).Msg("Failed to login with shared secret")
	} else {
		// TODO leave rooms with default puppet
		u.log.Info().Msg("Successfully automatically enabled custom puppet")
	}
}
